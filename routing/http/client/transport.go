package client

import (
	"fmt"
	"io"
	"net/http"
)

type ResponseBodyLimitedTransport struct {
	http.RoundTripper
	LimitBytes int64
}

func (r *ResponseBodyLimitedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.RoundTripper.RoundTrip(req)
	if resp != nil && resp.Body != nil {
		resp.Body = &limitReadCloser{
			limit:      r.LimitBytes,
			ReadCloser: resp.Body,
		}
	}
	return resp, err
}

type limitReadCloser struct {
	limit     int64
	bytesRead int64
	io.ReadCloser
}

func (l *limitReadCloser) Read(p []byte) (int, error) {
	n, err := l.ReadCloser.Read(p)
	l.bytesRead += int64(n)
	if l.bytesRead > l.limit {
		return 0, fmt.Errorf("reached read limit of %d bytes after reading %d bytes", l.limit, l.bytesRead)
	}
	return n, err
}
