package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/boxo/path"
)

func main() {
	flag.Usage = func() {
		fmt.Println("Usage: verified-fetch [flags] <path>")
		flag.PrintDefaults()
	}

	gatewayUrlPtr := flag.String("g", "https://trustless-gateway.link", "trustless gateway to download the CAR file from")
	userAgentPtr := flag.String("u", "", "user agent to use during the HTTP requests")
	outputPtr := flag.String("o", "out", "output path to store the fetched path")
	limitPtr := flag.Int64("l", 0, "file size limit for the gateway download (bytes)")
	flag.Parse()

	ipfsPath := flag.Arg(0)
	if len(ipfsPath) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	if err := fetch(*gatewayUrlPtr, ipfsPath, *outputPtr, *userAgentPtr, *limitPtr); err != nil {
		log.Fatal(err)
	}
}

func fetch(gatewayURL, ipfsPath, outputPath, userAgent string, limit int64) error {
	// Parse the given IPFS path to make sure it is valid.
	p, err := path.NewPath(ipfsPath)
	if err != nil {
		return err
	}

	// Create a custom [http.Client] with the given user agent and the limit.
	httpClient := &http.Client{
		Timeout: gateway.DefaultGetBlockTimeout,
		Transport: &limitedTransport{
			RoundTripper: http.DefaultTransport,
			limitBytes:   limit,
			userAgent:    userAgent,
		},
	}

	// Create the remote CAR gateway backend pointing to the given gateway URL and
	// using our [http.Client]. A custom [http.Client] is not required and the called
	// function would create a new one instead.
	backend, err := gateway.NewRemoteCarBackend([]string{gatewayURL}, httpClient)
	if err != nil {
		return err
	}

	// Resolve the given IPFS path to ensure that it is not mutable. This will
	// resolve both DNSLink and regular IPNS links. For the latter, it is
	// necessary that the given gateway supports [IPNS Record] response format.
	//
	// [IPNS Record]: https://www.iana.org/assignments/media-types/application/vnd.ipfs.ipns-record
	imPath, _, _, err := backend.ResolveMutable(context.Background(), p)
	if err != nil {
		return err
	}

	// Fetch the file or directory from the gateway. Since we're using a remote CAR
	// backend gateway, this call will internally fetch a CAR file from the remote
	// gateway and ensure that all blocks are present and verified.
	_, file, err := backend.GetAll(context.Background(), imPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the returned UnixFS file or directory to the file system.
	return files.WriteTo(file, outputPath)
}

type limitedTransport struct {
	http.RoundTripper
	limitBytes int64
	userAgent  string
}

func (r *limitedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if r.userAgent != "" {
		req.Header.Set("User-Agent", r.userAgent)
	}
	resp, err := r.RoundTripper.RoundTrip(req)
	if resp != nil && resp.Body != nil && r.limitBytes > 0 {
		resp.Body = &limitReadCloser{
			limit:      r.limitBytes,
			ReadCloser: resp.Body,
		}
	}
	return resp, err
}

type limitReadCloser struct {
	io.ReadCloser
	limit     int64
	bytesRead int64
}

func (l *limitReadCloser) Read(p []byte) (int, error) {
	n, err := l.ReadCloser.Read(p)
	l.bytesRead += int64(n)
	if l.bytesRead > l.limit {
		return 0, fmt.Errorf("reached read limit of %d bytes after reading %d bytes", l.limit, l.bytesRead)
	}
	return n, err
}
