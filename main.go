package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func readURLS(path string) ([]url.URL, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var out []url.URL
	defer fh.Close()
	scanner := bufio.NewScanner(fh)
	for scanner.Scan() {
		u, err := url.Parse(scanner.Text())
		if err != nil {
			return nil, fmt.Errorf("parse(%q): %w", scanner.Text(), err)
		}
		out = append(out, *u)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

var getters = map[string]func(*s3.S3, int, []url.URL) ([][]byte, error){
	"concurrent": getConcurrent,
	"pipelined":  getPipelined,
}

func main() {
	var (
		region  string
		mode    string
		threads int
		files   string
	)

	flag.StringVar(&region, "region", "us-west-2", "AWS Region")
	flag.StringVar(&mode, "mode", "concurrent", "Set the download mode")
	flag.IntVar(&threads, "threads", 32, "Number of concurrent download threads")
	flag.StringVar(&files, "files", "/dev/stdin", "List of S3 URLs to download")

	flag.Parse()

	getter, ok := getters[mode]
	if !ok {
		log.Fatalf("Invalid getter: %s", mode)
	}

	awscfg := aws.NewConfig().WithRegion(region)
	sess, err := session.NewSession(awscfg)
	if err != nil {
		log.Fatalf("aws: %s", err.Error())
	}

	s3svc := s3.New(sess)

	urls, err := readURLS(files)
	if err != nil {
		log.Fatalf("parse %q: %s", files, err.Error())
	}

	start := time.Now()
	out, err := getter(s3svc, threads, urls)
	if err != nil {
		log.Fatalf("getter: %s", err.Error())
	}

	hash := sha256.New()
	for i, o := range out {
		if o == nil {
			log.Fatalf("file %d (%s): no file downloaded", i, urls[i].String())
		}
		var lenbuf [8]byte
		binary.BigEndian.PutUint64(lenbuf[:], uint64(len(o)))
		hash.Write(lenbuf[:])
		hash.Write(o)
	}

	sum := hash.Sum(nil)

	log.Printf("downloaded elapsed=%s n=%d method=%s threads=%d csum=%s",
		time.Since(start),
		len(urls),
		mode,
		threads,
		hex.EncodeToString(sum),
	)
}
