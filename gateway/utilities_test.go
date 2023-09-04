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
	"time"

	"github.com/ipfs/boxo/blockservice"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/boxo/path"
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

type mockNamesysItem struct {
	path path.Path
	ttl  time.Duration
}

func newMockNamesysItem(p path.Path, ttl time.Duration) *mockNamesysItem {
	return &mockNamesysItem{path: p, ttl: ttl}
}

type mockNamesys map[string]*mockNamesysItem

func (m mockNamesys) Resolve(ctx context.Context, p path.Path, opts ...namesys.ResolveOption) (result namesys.ResolveResult, err error) {
	cfg := namesys.DefaultResolveOptions()
	for _, o := range opts {
		o(&cfg)
	}
	depth := cfg.Depth
	if depth == namesys.UnlimitedDepth {
		// max uint
		depth = ^uint(0)
	}
	var (
		value path.Path
		ttl   time.Duration
	)
	name := path.SegmentsToString(p.Segments()[:2]...)
	for strings.HasPrefix(name, "/ipns/") {
		if depth == 0 {
			return namesys.ResolveResult{Path: value, TTL: ttl}, namesys.ErrResolveRecursion
		}
		depth--

		v, ok := m[name]
		if !ok {
			return namesys.ResolveResult{}, namesys.ErrResolveFailed
		}
		value = v.path
		ttl = v.ttl
		name = value.String()
	}

	value, err = path.Join(value, p.Segments()[2:]...)
	return namesys.ResolveResult{Path: value, TTL: ttl}, err
}

func (m mockNamesys) ResolveAsync(ctx context.Context, p path.Path, opts ...namesys.ResolveOption) <-chan namesys.ResolveAsyncResult {
	out := make(chan namesys.ResolveAsyncResult, 1)
	res, err := m.Resolve(ctx, p, opts...)
	out <- namesys.ResolveAsyncResult{Path: res.Path, TTL: res.TTL, LastMod: res.LastMod, Err: err}
	close(out)
	return out
}

func (m mockNamesys) Publish(ctx context.Context, name crypto.PrivKey, value path.Path, opts ...namesys.PublishOption) error {
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

func (mb *mockBackend) Get(ctx context.Context, immutablePath path.ImmutablePath, ranges ...ByteRange) (ContentPathMetadata, *GetResponse, error) {
	return mb.gw.Get(ctx, immutablePath, ranges...)
}

func (mb *mockBackend) GetAll(ctx context.Context, immutablePath path.ImmutablePath) (ContentPathMetadata, files.Node, error) {
	return mb.gw.GetAll(ctx, immutablePath)
}

func (mb *mockBackend) GetBlock(ctx context.Context, immutablePath path.ImmutablePath) (ContentPathMetadata, files.File, error) {
	return mb.gw.GetBlock(ctx, immutablePath)
}

func (mb *mockBackend) Head(ctx context.Context, immutablePath path.ImmutablePath) (ContentPathMetadata, *HeadResponse, error) {
	return mb.gw.Head(ctx, immutablePath)
}

func (mb *mockBackend) GetCAR(ctx context.Context, immutablePath path.ImmutablePath, params CarParams) (ContentPathMetadata, io.ReadCloser, error) {
	return mb.gw.GetCAR(ctx, immutablePath, params)
}

func (mb *mockBackend) ResolveMutable(ctx context.Context, p path.Path) (path.ImmutablePath, time.Duration, time.Time, error) {
	return mb.gw.ResolveMutable(ctx, p)
}

func (mb *mockBackend) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	return nil, routing.ErrNotSupported
}

func (mb *mockBackend) GetDNSLinkRecord(ctx context.Context, hostname string) (path.Path, error) {
	if mb.namesys != nil {
		p, err := path.NewPath("/ipns/" + hostname)
		if err != nil {
			return nil, err
		}
		res, err := mb.namesys.Resolve(ctx, p, namesys.ResolveWithDepth(1))
		if err == namesys.ErrResolveRecursion {
			err = nil
		}
		p = res.Path
		return p, err
	}

	return nil, errors.New("not implemented")
}

func (mb *mockBackend) IsCached(ctx context.Context, p path.Path) bool {
	return mb.gw.IsCached(ctx, p)
}

func (mb *mockBackend) ResolvePath(ctx context.Context, immutablePath path.ImmutablePath) (ContentPathMetadata, error) {
	return mb.gw.ResolvePath(ctx, immutablePath)
}

func (mb *mockBackend) resolvePathNoRootsReturned(ctx context.Context, ip path.Path) (path.ImmutablePath, error) {
	var imPath path.ImmutablePath
	var err error
	if ip.Mutable() {
		imPath, _, _, err = mb.ResolveMutable(ctx, ip)
		if err != nil {
			return path.ImmutablePath{}, err
		}
	} else {
		imPath, err = path.NewImmutablePath(ip)
		if err != nil {
			return path.ImmutablePath{}, err
		}
	}

	md, err := mb.ResolvePath(ctx, imPath)
	if err != nil {
		return path.ImmutablePath{}, err
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
