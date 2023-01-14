package client

import (
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
)

type ResponseBodyLimitedTransport struct {
	http.RoundTripper
	LimitBytes int64
	UserAgent  string
}

func (r *ResponseBodyLimitedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if r.UserAgent != "" {
		req.Header.Set("User-Agent", r.UserAgent)
	}
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

// ImportPath is the canonical import path that allows us to identify
// official client builds vs modified forks, and use that info in User-Agent header.
const ImportPath = "github.com/ipfs/go-libipfs/routing/http/client"

// moduleVersion returns a useful user agent version string allowing us to
// identify requests coming from officialxl releases of this module.
func moduleVersion() string {
	var module *debug.Module
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}

	// find this client in the dependency list
	// of the app that has it in go.mod
	for _, dep := range bi.Deps {
		if dep.Path == ImportPath {
			module = dep
			break
		}
	}

	if module == nil {
		return ""
	}
	ua := ImportPath + "/" + module.Version
	if module.Sum != "" {
		ua += "/" + module.Sum
	}
	return ua
}
