package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/ipfs/boxo/blockservice"
	nsopts "github.com/ipfs/boxo/coreiface/options/namesys"
	ipath "github.com/ipfs/boxo/coreiface/path"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/namesys"
	path "github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustNewRequest(t *testing.T, method string, path string, body io.Reader) *http.Request {
	r, err := http.NewRequest(http.MethodGet, path, body)
	require.NoError(t, err)
	return r
}

func mustDoWithoutRedirect(t *testing.T, req *http.Request) *http.Response {
	errNoRedirect := errors.New("without-redirect")
	c := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return errNoRedirect
		},
	}
	res, err := c.Do(req)
	require.True(t, err == nil || errors.Is(err, errNoRedirect))
	return res
}

func mustDo(t *testing.T, req *http.Request) *http.Response {
	c := &http.Client{}
	res, err := c.Do(req)
	require.NoError(t, err)
	return res
}

type mockNamesys map[string]path.Path

func (m mockNamesys) Resolve(ctx context.Context, name string, opts ...nsopts.ResolveOpt) (value path.Path, err error) {
	cfg := nsopts.DefaultResolveOpts()
	for _, o := range opts {
		o(&cfg)
	}
	depth := cfg.Depth
	if depth == nsopts.UnlimitedDepth {
		// max uint
		depth = ^uint(0)
	}
	for strings.HasPrefix(name, "/ipns/") {
		if depth == 0 {
			return value, namesys.ErrResolveRecursion
		}
		depth--

		var ok bool
		value, ok = m[name]
		if !ok {
			return "", namesys.ErrResolveFailed
		}
		name = value.String()
	}
	return value, nil
}

func (m mockNamesys) ResolveAsync(ctx context.Context, name string, opts ...nsopts.ResolveOpt) <-chan namesys.Result {
	out := make(chan namesys.Result, 1)
	v, err := m.Resolve(ctx, name, opts...)
	out <- namesys.Result{Path: v, Err: err}
	close(out)
	return out
}

func (m mockNamesys) Publish(ctx context.Context, name crypto.PrivKey, value path.Path, opts ...nsopts.PublishOption) error {
	return errors.New("not implemented for mockNamesys")
}

func (m mockNamesys) GetResolver(subs string) (namesys.Resolver, bool) {
	return nil, false
}

type mockBackend struct {
	gw      IPFSBackend
	namesys mockNamesys
}

var _ IPFSBackend = (*mockBackend)(nil)

func newMockBackend(t *testing.T, fixturesFile string) (*mockBackend, cid.Cid) {
	r, err := os.Open(filepath.Join("./testdata", fixturesFile))
	assert.NoError(t, err)

	blockStore, err := carblockstore.NewReadOnly(r, nil)
	assert.NoError(t, err)

	t.Cleanup(func() {
		blockStore.Close()
		r.Close()
	})

	cids, err := blockStore.Roots()
	assert.NoError(t, err)
	assert.Len(t, cids, 1)

	blockService := blockservice.New(blockStore, offline.Exchange(blockStore))

	n := mockNamesys{}
	backend, err := NewBlocksBackend(blockService, WithNameSystem(n))
	if err != nil {
		t.Fatal(err)
	}

	return &mockBackend{
		gw:      backend,
		namesys: n,
	}, cids[0]
}

func (mb *mockBackend) Get(ctx context.Context, immutablePath ImmutablePath, ranges ...ByteRange) (ContentPathMetadata, *GetResponse, error) {
	return mb.gw.Get(ctx, immutablePath, ranges...)
}

func (mb *mockBackend) GetAll(ctx context.Context, immutablePath ImmutablePath) (ContentPathMetadata, files.Node, error) {
	return mb.gw.GetAll(ctx, immutablePath)
}

func (mb *mockBackend) GetBlock(ctx context.Context, immutablePath ImmutablePath) (ContentPathMetadata, files.File, error) {
	return mb.gw.GetBlock(ctx, immutablePath)
}

func (mb *mockBackend) Head(ctx context.Context, immutablePath ImmutablePath) (ContentPathMetadata, files.Node, error) {
	return mb.gw.Head(ctx, immutablePath)
}

func (mb *mockBackend) GetCAR(ctx context.Context, immutablePath ImmutablePath, params CarParams) (ContentPathMetadata, io.ReadCloser, error) {
	return mb.gw.GetCAR(ctx, immutablePath, params)
}

func (mb *mockBackend) ResolveMutable(ctx context.Context, p ipath.Path) (ImmutablePath, error) {
	return mb.gw.ResolveMutable(ctx, p)
}

func (mb *mockBackend) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	return nil, routing.ErrNotSupported
}

func (mb *mockBackend) GetDNSLinkRecord(ctx context.Context, hostname string) (ipath.Path, error) {
	if mb.namesys != nil {
		p, err := mb.namesys.Resolve(ctx, "/ipns/"+hostname, nsopts.Depth(1))
		if err == namesys.ErrResolveRecursion {
			err = nil
		}
		return ipath.New(p.String()), err
	}

	return nil, errors.New("not implemented")
}

func (mb *mockBackend) IsCached(ctx context.Context, p ipath.Path) bool {
	return mb.gw.IsCached(ctx, p)
}

func (mb *mockBackend) ResolvePath(ctx context.Context, immutablePath ImmutablePath) (ContentPathMetadata, error) {
	return mb.gw.ResolvePath(ctx, immutablePath)
}

func (mb *mockBackend) resolvePathNoRootsReturned(ctx context.Context, ip ipath.Path) (ipath.Resolved, error) {
	var imPath ImmutablePath
	var err error
	if ip.Mutable() {
		imPath, err = mb.ResolveMutable(ctx, ip)
		if err != nil {
			return nil, err
		}
	} else {
		imPath, err = NewImmutablePath(ip)
		if err != nil {
			return nil, err
		}
	}

	md, err := mb.ResolvePath(ctx, imPath)
	if err != nil {
		return nil, err
	}
	return md.LastSegment, nil
}

func newTestServerAndNode(t *testing.T, ns mockNamesys, fixturesFile string) (*httptest.Server, *mockBackend, cid.Cid) {
	backend, root := newMockBackend(t, fixturesFile)
	ts := newTestServer(t, backend)
	return ts, backend, root
}

func newTestServer(t *testing.T, backend IPFSBackend) *httptest.Server {
	return newTestServerWithConfig(t, backend, Config{
		Headers:               map[string][]string{},
		DeserializedResponses: true,
	})
}

func newTestServerWithConfig(t *testing.T, backend IPFSBackend, config Config) *httptest.Server {
	AddAccessControlHeaders(config.Headers)

	handler := NewHandler(config, backend)
	mux := http.NewServeMux()
	mux.Handle("/ipfs/", handler)
	mux.Handle("/ipns/", handler)
	handler = NewHostnameHandler(config, backend, mux)

	ts := httptest.NewServer(handler)
	t.Cleanup(func() { ts.Close() })
	t.Logf("test server url: %s", ts.URL)

	return ts
}

func matchPathOrBreadcrumbs(s string, expected string) bool {
	matched, _ := regexp.MatchString("Index of(\n|\r\n)[\t ]*"+regexp.QuoteMeta(expected), s)
	return matched
}
