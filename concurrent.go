package main

import (
	"io/ioutil"
	"net/url"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/sync/errgroup"
)

type job struct {
	idx int
	u   url.URL
}

type result struct {
	job  job
	data []byte
	err  error
}

func getConcurrent(svc *s3.S3, conc int, urls []url.URL) ([][]byte, error) {
	var grp errgroup.Group
	jobs := make(chan job)
	done := make(chan result)
	out := make([][]byte, len(urls))

	grp.Go(func() error {
		for got := range done {
			if got.err != nil {
				return got.err
			}
			out[got.job.idx] = got.data
		}
		return nil
	})

	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		grp.Go(func() error {
			defer wg.Done()
			for j := range jobs {
				req := s3.GetObjectInput{
					Bucket: aws.String(j.u.Host),
					Key:    aws.String(j.u.Path),
				}
				resp, err := svc.GetObject(&req)
				res := result{err: err, job: j}
				if err == nil {
					res.data, res.err = ioutil.ReadAll(resp.Body)
					resp.Body.Close()
				}
				done <- res
			}
			return nil
		})
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	grp.Go(func() error {
		defer close(jobs)
		for i, j := range urls {
			jobs <- job{i, j}
		}
		return nil
	})

	return out, grp.Wait()
}
