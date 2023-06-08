package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"testing"
	"time"

	ipath "github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/path/resolver"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/assert"
)

func TestEtagMatch(t *testing.T) {
	for _, test := range []struct {
		header   string // value in If-None-Match HTTP header
		cidEtag  string
		dirEtag  string
		expected bool // expected result of etagMatch(header, cidEtag, dirEtag)
	}{
		{"", `"etag"`, "", false},                        // no If-None-Match
		{"", "", `"etag"`, false},                        // no If-None-Match
		{`"etag"`, `"etag"`, "", true},                   // file etag match
		{`W/"etag"`, `"etag"`, "", true},                 // file etag match
		{`"foo", W/"bar", W/"etag"`, `"etag"`, "", true}, // file etag match (array)
		{`"foo",W/"bar",W/"etag"`, `"etag"`, "", true},   // file etag match (compact array)
		{`"etag"`, "", `W/"etag"`, true},                 // dir etag match
		{`"etag"`, "", `W/"etag"`, true},                 // dir etag match
		{`W/"etag"`, "", `W/"etag"`, true},               // dir etag match
		{`*`, `"etag"`, "", true},                        // wildcard etag match
	} {
		result := etagMatch(test.header, test.cidEtag, test.dirEtag)
		assert.Equalf(t, test.expected, result, "etagMatch(%q, %q, %q)", test.header, test.cidEtag, test.dirEtag)
	}
}

type errorMockBackend struct {
	err error
}

func (mb *errorMockBackend) Get(ctx context.Context, path ImmutablePath, getRange ...ByteRange) (ContentPathMetadata, *GetResponse, error) {
	return ContentPathMetadata{}, nil, mb.err
}

func (mb *errorMockBackend) GetAll(ctx context.Context, path ImmutablePath) (ContentPathMetadata, files.Node, error) {
	return ContentPathMetadata{}, nil, mb.err
}

func (mb *errorMockBackend) GetBlock(ctx context.Context, path ImmutablePath) (ContentPathMetadata, files.File, error) {
	return ContentPathMetadata{}, nil, mb.err
}

func (mb *errorMockBackend) Head(ctx context.Context, path ImmutablePath) (ContentPathMetadata, files.Node, error) {
	return ContentPathMetadata{}, nil, mb.err
}

func (mb *errorMockBackend) GetCAR(ctx context.Context, path ImmutablePath, params CarParams) (ContentPathMetadata, io.ReadCloser, error) {
	return ContentPathMetadata{}, nil, mb.err
}

func (mb *errorMockBackend) ResolveMutable(ctx context.Context, path ipath.Path) (ImmutablePath, error) {
	return ImmutablePath{}, mb.err
}

func (mb *errorMockBackend) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	return nil, mb.err
}

func (mb *errorMockBackend) GetDNSLinkRecord(ctx context.Context, hostname string) (ipath.Path, error) {
	return nil, mb.err
}

func (mb *errorMockBackend) IsCached(ctx context.Context, p ipath.Path) bool {
	return false
}

func (mb *errorMockBackend) ResolvePath(ctx context.Context, path ImmutablePath) (ContentPathMetadata, error) {
	return ContentPathMetadata{}, mb.err
}

func TestGatewayBadRequestInvalidPath(t *testing.T) {
	backend, _ := newMockBackend(t)
	ts := newTestServer(t, backend)
	t.Logf("test server url: %s", ts.URL)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipfs/QmInvalid/Path", nil)
	assert.NoError(t, err)

	res, err := ts.Client().Do(req)
	assert.NoError(t, err)

	assert.Equal(t, http.StatusBadRequest, res.StatusCode)
}

func TestErrorBubblingFromBackend(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		err    error
		status int
	}{
		{"404 Not Found from IPLD", &ipld.ErrNotFound{}, http.StatusNotFound},
		{"404 Not Found from path resolver", resolver.ErrNoLink{}, http.StatusNotFound},
		{"502 Bad Gateway", ErrBadGateway, http.StatusBadGateway},
		{"504 Gateway Timeout", ErrGatewayTimeout, http.StatusGatewayTimeout},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &errorMockBackend{err: fmt.Errorf("wrapped for testing purposes: %w", test.err)}
			ts := newTestServer(t, backend)
			t.Logf("test server url: %s", ts.URL)

			req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipns/en.wikipedia-on-ipfs.org", nil)
			assert.NoError(t, err)

			res, err := ts.Client().Do(req)
			assert.NoError(t, err)
			assert.Equal(t, test.status, res.StatusCode)
		})
	}

	for _, test := range []struct {
		name         string
		err          error
		status       int
		headerName   string
		headerValue  string
		headerLength int // how many times was headerName set
	}{
		{"429 Too Many Requests without Retry-After header", ErrTooManyRequests, http.StatusTooManyRequests, "Retry-After", "", 0},
		{"429 Too Many Requests without Retry-After header", NewErrorRetryAfter(ErrTooManyRequests, 0*time.Second), http.StatusTooManyRequests, "Retry-After", "", 0},
		{"429 Too Many Requests with Retry-After header", NewErrorRetryAfter(ErrTooManyRequests, 3600*time.Second), http.StatusTooManyRequests, "Retry-After", "3600", 1},
	} {
		backend := &errorMockBackend{err: fmt.Errorf("wrapped for testing purposes: %w", test.err)}
		ts := newTestServer(t, backend)
		t.Logf("test server url: %s", ts.URL)

		req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipns/en.wikipedia-on-ipfs.org", nil)
		assert.NoError(t, err)

		res, err := ts.Client().Do(req)
		assert.NoError(t, err)
		assert.Equal(t, test.status, res.StatusCode)
		assert.Equal(t, test.headerValue, res.Header.Get(test.headerName))
		assert.Equal(t, test.headerLength, len(res.Header.Values(test.headerName)))
	}
}

type panicMockBackend struct {
	panicOnHostnameHandler bool
}

func (mb *panicMockBackend) Get(ctx context.Context, immutablePath ImmutablePath, ranges ...ByteRange) (ContentPathMetadata, *GetResponse, error) {
	panic("i am panicking")
}

func (mb *panicMockBackend) GetAll(ctx context.Context, immutablePath ImmutablePath) (ContentPathMetadata, files.Node, error) {
	panic("i am panicking")
}

func (mb *panicMockBackend) GetBlock(ctx context.Context, immutablePath ImmutablePath) (ContentPathMetadata, files.File, error) {
	panic("i am panicking")
}

func (mb *panicMockBackend) Head(ctx context.Context, immutablePath ImmutablePath) (ContentPathMetadata, files.Node, error) {
	panic("i am panicking")
}

func (mb *panicMockBackend) GetCAR(ctx context.Context, immutablePath ImmutablePath, params CarParams) (ContentPathMetadata, io.ReadCloser, error) {
	panic("i am panicking")
}

func (mb *panicMockBackend) ResolveMutable(ctx context.Context, p ipath.Path) (ImmutablePath, error) {
	panic("i am panicking")
}

func (mb *panicMockBackend) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	panic("i am panicking")
}

func (mb *panicMockBackend) GetDNSLinkRecord(ctx context.Context, hostname string) (ipath.Path, error) {
	// GetDNSLinkRecord is also called on the WithHostname handler. We have this option
	// to disable panicking here so we can test if both the regular gateway handler
	// and the hostname handler can handle panics.
	if mb.panicOnHostnameHandler {
		panic("i am panicking")
	}

	return nil, errors.New("not implemented")
}

func (mb *panicMockBackend) IsCached(ctx context.Context, p ipath.Path) bool {
	panic("i am panicking")
}

func (mb *panicMockBackend) ResolvePath(ctx context.Context, immutablePath ImmutablePath) (ContentPathMetadata, error) {
	panic("i am panicking")
}

func TestGatewayStatusCodeOnPanic(t *testing.T) {
	backend := &panicMockBackend{}
	ts := newTestServer(t, backend)
	t.Logf("test server url: %s", ts.URL)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipfs/bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e", nil)
	assert.NoError(t, err)

	res, err := ts.Client().Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, res.StatusCode)
}

func TestGatewayStatusCodeOnHostnamePanic(t *testing.T) {
	backend := &panicMockBackend{panicOnHostnameHandler: true}
	ts := newTestServer(t, backend)
	t.Logf("test server url: %s", ts.URL)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipfs/bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e", nil)
	assert.NoError(t, err)

	res, err := ts.Client().Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, res.StatusCode)
}
