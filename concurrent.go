package main

import (
	"io/ioutil"
	"net/url"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/sync/errgroup"
)

type result struct {
	url  *url.URL
	data []byte
	err  error
}

func getConcurrent(svc *s3.S3, conc int, urls []url.URL) error {
	var grp errgroup.Group
	jobs := make(chan url.URL)
	done := make(chan result)
	grp.Go(func() error {
		for range done {
			// do something with the result
		}
		return nil
	})
	grp.Go(func() error {
		defer close(jobs)
		for _, j := range urls {
			jobs <- j
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
					Bucket: aws.String(j.Host),
					Key:    aws.String(j.Path),
				}
				resp, err := svc.GetObject(&req)
				var res result
				res.err = err
				if err != nil {
					res.data, res.err = ioutil.ReadAll(resp.Body)
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
	return grp.Wait()
}
