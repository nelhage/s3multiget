package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sync"

	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/sync/errgroup"
)

type cbFunc func(ctx context.Context, r *http.Response, err error)

type pipelinedConnection struct {
	conn net.Conn
	rd   *bufio.Reader
	ctx  context.Context
	grp  *errgroup.Group

	requests chan pipelinedRequest
	inflight chan pipelinedRequest
}

type pipelinedRequest struct {
	req *http.Request
	cb  cbFunc
}

func (p *pipelinedConnection) queueRequest(r *http.Request, cb cbFunc) {
	select {
	case p.requests <- pipelinedRequest{r, cb}:
	case <-p.ctx.Done():
	}
}

const maxInflight = 32

var errConnectionShutdown = errors.New("Connection shut down")

func newPipeline(ctx context.Context, conn net.Conn) *pipelinedConnection {
	grp, ctx := errgroup.WithContext(ctx)
	p := &pipelinedConnection{
		grp:      grp,
		conn:     conn,
		rd:       bufio.NewReader(conn),
		ctx:      ctx,
		requests: make(chan pipelinedRequest),
		inflight: make(chan pipelinedRequest, maxInflight),
	}

	p.grp.Go(func() error {
		defer close(p.inflight)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case r, ok := <-p.requests:
				if !ok {
					return nil
				}
				if err := r.req.Write(p.conn); err != nil {
					return err
				}
				select {
				case p.inflight <- r:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	})

	p.grp.Go(func() error {
		defer func() {
			for r := range p.inflight {
				r.cb(ctx, nil, errConnectionShutdown)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case r, ok := <-p.inflight:
				if !ok {
					return nil
				}
				resp, err := http.ReadResponse(p.rd, r.req)
				if err != nil {
					return err
				}

				r.cb(ctx, resp, nil)
				if err := resp.Body.Close(); err != nil {
					return err
				}
			}
		}
	})

	return p
}

func (p *pipelinedConnection) Close() error {
	close(p.requests)
	err := p.grp.Wait()
	p.conn.Close()
	return err
}

func getPipelined(svc *s3.S3, conc int, urls []url.URL) ([][]byte, error) {
	jobs := make(chan job)
	done := make(chan result)
	var grp errgroup.Group

	var wg sync.WaitGroup

	endpoint, err := url.Parse(svc.Endpoint)
	if err != nil {
		return nil, err
	}

	out := make([][]byte, len(urls))
	grp.Go(func() error {
		var err error
		for got := range done {
			if got.err != nil {
				err = got.err
				continue
			}
			out[got.job.idx] = got.data
		}
		return err
	})

	for i := 0; i < conc; i++ {
		wg.Add(1)
		grp.Go(func() error {
			defer wg.Done()
			var pipe *pipelinedConnection
			conn, err := tls.Dial("tcp", fmt.Sprintf("%s:%d", endpoint.Host, 443), nil)
			if err != nil {
				return err
			}
			pipe = newPipeline(context.Background(), conn)
			for j := range jobs {
				req, out := svc.GetObjectRequest(&s3.GetObjectInput{
					Bucket: &j.u.Host,
					Key:    &j.u.Path,
				})
				if err := req.Sign(); err != nil {
					return err
				}
				j := j
				pipe.queueRequest(req.HTTPRequest, func(ctx context.Context, resp *http.Response, err error) {
					r := result{job: j, err: err}
					defer func() { done <- r }()

					if err != nil {
						return
					}
					req.HTTPResponse = resp
					req.Handlers.UnmarshalMeta.Run(req)
					req.Handlers.ValidateResponse.Run(req)
					if req.Error == nil {
						req.Handlers.Unmarshal.Run(req)
					}
					if req.Error != nil {
						r.err = req.Error
						return
					}
					defer out.Body.Close()
					r.data, r.err = ioutil.ReadAll(out.Body)
				})
			}
			return pipe.Close()
		})
	}

	go func() {
		wg.Wait()
		close(done)
	}()
	grp.Go(func() error {
		defer close(jobs)
		for i, u := range urls {
			jobs <- job{i, u}
		}
		return nil
	})

	return out, grp.Wait()
}
