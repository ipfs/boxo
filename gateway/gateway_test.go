package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/ipfs/boxo/blockservice"
	nsopts "github.com/ipfs/boxo/coreiface/options/namesys"
	ipath "github.com/ipfs/boxo/coreiface/path"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/files"
	carblockstore "github.com/ipfs/boxo/ipld/car/v2/blockstore"
	"github.com/ipfs/boxo/namesys"
	path "github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/stretchr/testify/assert"
)

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

type mockAPI struct {
	gw      IPFSBackend
	namesys mockNamesys
}

var _ IPFSBackend = (*mockAPI)(nil)

func newMockAPI(t *testing.T) (*mockAPI, cid.Cid) {
	r, err := os.Open("./testdata/fixtures.car")
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
	gwApi, err := NewBlocksGateway(blockService, WithNameSystem(n))
	if err != nil {
		t.Fatal(err)
	}

	return &mockAPI{
		gw:      gwApi,
		namesys: n,
	}, cids[0]
}

func (api *mockAPI) Get(ctx context.Context, immutablePath ImmutablePath, ranges ...ByteRange) (ContentPathMetadata, *GetResponse, error) {
	return api.gw.Get(ctx, immutablePath, ranges...)
}

func (api *mockAPI) GetAll(ctx context.Context, immutablePath ImmutablePath) (ContentPathMetadata, files.Node, error) {
	return api.gw.GetAll(ctx, immutablePath)
}

func (api *mockAPI) GetBlock(ctx context.Context, immutablePath ImmutablePath) (ContentPathMetadata, files.File, error) {
	return api.gw.GetBlock(ctx, immutablePath)
}

func (api *mockAPI) Head(ctx context.Context, immutablePath ImmutablePath) (ContentPathMetadata, files.Node, error) {
	return api.gw.Head(ctx, immutablePath)
}

func (api *mockAPI) GetCAR(ctx context.Context, immutablePath ImmutablePath) (ContentPathMetadata, io.ReadCloser, <-chan error, error) {
	return api.gw.GetCAR(ctx, immutablePath)
}

func (api *mockAPI) ResolveMutable(ctx context.Context, p ipath.Path) (ImmutablePath, error) {
	return api.gw.ResolveMutable(ctx, p)
}

func (api *mockAPI) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	return nil, routing.ErrNotSupported
}

func (api *mockAPI) GetDNSLinkRecord(ctx context.Context, hostname string) (ipath.Path, error) {
	if api.namesys != nil {
		p, err := api.namesys.Resolve(ctx, "/ipns/"+hostname, nsopts.Depth(1))
		if err == namesys.ErrResolveRecursion {
			err = nil
		}
		return ipath.New(p.String()), err
	}

	return nil, errors.New("not implemented")
}

func (api *mockAPI) IsCached(ctx context.Context, p ipath.Path) bool {
	return api.gw.IsCached(ctx, p)
}

func (api *mockAPI) ResolvePath(ctx context.Context, immutablePath ImmutablePath) (ContentPathMetadata, error) {
	return api.gw.ResolvePath(ctx, immutablePath)
}

func (api *mockAPI) resolvePathNoRootsReturned(ctx context.Context, ip ipath.Path) (ipath.Resolved, error) {
	var imPath ImmutablePath
	var err error
	if ip.Mutable() {
		imPath, err = api.ResolveMutable(ctx, ip)
		if err != nil {
			return nil, err
		}
	} else {
		imPath, err = NewImmutablePath(ip)
		if err != nil {
			return nil, err
		}
	}

	md, err := api.ResolvePath(ctx, imPath)
	if err != nil {
		return nil, err
	}
	return md.LastSegment, nil
}

func doWithoutRedirect(req *http.Request) (*http.Response, error) {
	tag := "without-redirect"
	c := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return errors.New(tag)
		},
	}
	res, err := c.Do(req)
	if err != nil && !strings.Contains(err.Error(), tag) {
		return nil, err
	}
	return res, nil
}

func newTestServerAndNode(t *testing.T, ns mockNamesys) (*httptest.Server, *mockAPI, cid.Cid) {
	api, root := newMockAPI(t)
	ts := newTestServer(t, api)
	return ts, api, root
}

func newTestServer(t *testing.T, api IPFSBackend) *httptest.Server {
	return newTestServerWithConfig(t, api, Config{
		Headers:               map[string][]string{},
		DeserializedResponses: true,
	})
}

func newTestServerWithConfig(t *testing.T, api IPFSBackend, config Config) *httptest.Server {
	AddAccessControlHeaders(config.Headers)

	handler := NewHandler(config, api)
	mux := http.NewServeMux()
	mux.Handle("/ipfs/", handler)
	mux.Handle("/ipns/", handler)
	handler = WithHostname(config, api, mux)

	ts := httptest.NewServer(handler)
	t.Cleanup(func() { ts.Close() })

	return ts
}

func matchPathOrBreadcrumbs(s string, expected string) bool {
	matched, _ := regexp.MatchString("Index of(\n|\r\n)[\t ]*"+regexp.QuoteMeta(expected), s)
	return matched
}

func TestGatewayGet(t *testing.T) {
	ts, api, root := newTestServerAndNode(t, nil)
	t.Logf("test server url: %s", ts.URL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	k, err := api.resolvePathNoRootsReturned(ctx, ipath.Join(ipath.IpfsPath(root), t.Name(), "fnord"))
	assert.NoError(t, err)

	api.namesys["/ipns/example.com"] = path.FromCid(k.Cid())
	api.namesys["/ipns/working.example.com"] = path.FromString(k.String())
	api.namesys["/ipns/double.example.com"] = path.FromString("/ipns/working.example.com")
	api.namesys["/ipns/triple.example.com"] = path.FromString("/ipns/double.example.com")
	api.namesys["/ipns/broken.example.com"] = path.FromString("/ipns/" + k.Cid().String())
	// We picked .man because:
	// 1. It's a valid TLD.
	// 2. Go treats it as the file extension for "man" files (even though
	//    nobody actually *uses* this extension, AFAIK).
	//
	// Unfortunately, this may not work on all platforms as file type
	// detection is platform dependent.
	api.namesys["/ipns/example.man"] = path.FromString(k.String())

	t.Log(ts.URL)
	for _, test := range []struct {
		host   string
		path   string
		status int
		text   string
	}{
		{"127.0.0.1:8080", "/", http.StatusNotFound, "404 page not found\n"},
		{"127.0.0.1:8080", "/ipfs", http.StatusBadRequest, "invalid path \"/ipfs/\": not enough path components\n"},
		{"127.0.0.1:8080", "/ipns", http.StatusBadRequest, "invalid path \"/ipns/\": not enough path components\n"},
		{"127.0.0.1:8080", "/" + k.Cid().String(), http.StatusNotFound, "404 page not found\n"},
		{"127.0.0.1:8080", "/ipfs/this-is-not-a-cid", http.StatusBadRequest, "invalid path \"/ipfs/this-is-not-a-cid\": invalid CID: invalid cid: illegal base32 data at input byte 3\n"},
		{"127.0.0.1:8080", k.String(), http.StatusOK, "fnord"},
		{"127.0.0.1:8080", "/ipns/nxdomain.example.com", http.StatusInternalServerError, "failed to resolve /ipns/nxdomain.example.com: " + namesys.ErrResolveFailed.Error() + "\n"},
		{"127.0.0.1:8080", "/ipns/%0D%0A%0D%0Ahello", http.StatusInternalServerError, "failed to resolve /ipns/\\r\\n\\r\\nhello: " + namesys.ErrResolveFailed.Error() + "\n"},
		{"127.0.0.1:8080", "/ipns/k51qzi5uqu5djucgtwlxrbfiyfez1nb0ct58q5s4owg6se02evza05dfgi6tw5", http.StatusInternalServerError, "failed to resolve /ipns/k51qzi5uqu5djucgtwlxrbfiyfez1nb0ct58q5s4owg6se02evza05dfgi6tw5: " + namesys.ErrResolveFailed.Error() + "\n"},
		{"127.0.0.1:8080", "/ipns/example.com", http.StatusOK, "fnord"},
		{"example.com", "/", http.StatusOK, "fnord"},

		{"working.example.com", "/", http.StatusOK, "fnord"},
		{"double.example.com", "/", http.StatusOK, "fnord"},
		{"triple.example.com", "/", http.StatusOK, "fnord"},
		{"working.example.com", k.String(), http.StatusNotFound, "failed to resolve /ipns/working.example.com" + k.String() + ": no link named \"ipfs\" under " + k.Cid().String() + "\n"},
		{"broken.example.com", "/", http.StatusInternalServerError, "failed to resolve /ipns/broken.example.com/: " + namesys.ErrResolveFailed.Error() + "\n"},
		{"broken.example.com", k.String(), http.StatusInternalServerError, "failed to resolve /ipns/broken.example.com" + k.String() + ": " + namesys.ErrResolveFailed.Error() + "\n"},
		// This test case ensures we don't treat the TLD as a file extension.
		{"example.man", "/", http.StatusOK, "fnord"},
	} {
		testName := "http://" + test.host + test.path
		t.Run(testName, func(t *testing.T) {
			var c http.Client
			r, err := http.NewRequest(http.MethodGet, ts.URL+test.path, nil)
			assert.NoError(t, err)
			r.Host = test.host
			resp, err := c.Do(r)
			assert.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("Content-Type"))
			body, err := io.ReadAll(resp.Body)
			assert.NoError(t, err)
			assert.Equal(t, test.status, resp.StatusCode, "body", body)
			assert.Equal(t, test.text, string(body))
		})
	}
}

func TestUriQueryRedirect(t *testing.T) {
	ts, _, _ := newTestServerAndNode(t, mockNamesys{})

	cid := "QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR"
	for _, test := range []struct {
		path     string
		status   int
		location string
	}{
		// - Browsers will send original URI in URL-escaped form
		// - We expect query parameters to be persisted
		// - We drop fragments, as those should not be sent by a browser
		{"/ipfs/?uri=ipfs%3A%2F%2FQmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco%2Fwiki%2FFoo_%C4%85%C4%99.html%3Ffilename%3Dtest-%C4%99.html%23header-%C4%85", http.StatusMovedPermanently, "/ipfs/QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco/wiki/Foo_%c4%85%c4%99.html?filename=test-%c4%99.html"},
		{"/ipfs/?uri=ipns%3A%2F%2Fexample.com%2Fwiki%2FFoo_%C4%85%C4%99.html%3Ffilename%3Dtest-%C4%99.html", http.StatusMovedPermanently, "/ipns/example.com/wiki/Foo_%c4%85%c4%99.html?filename=test-%c4%99.html"},
		{"/ipfs/?uri=ipfs://" + cid, http.StatusMovedPermanently, "/ipfs/" + cid},
		{"/ipfs?uri=ipfs://" + cid, http.StatusMovedPermanently, "/ipfs/?uri=ipfs://" + cid},
		{"/ipfs/?uri=ipns://" + cid, http.StatusMovedPermanently, "/ipns/" + cid},
		{"/ipns/?uri=ipfs%3A%2F%2FQmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco%2Fwiki%2FFoo_%C4%85%C4%99.html%3Ffilename%3Dtest-%C4%99.html%23header-%C4%85", http.StatusMovedPermanently, "/ipfs/QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco/wiki/Foo_%c4%85%c4%99.html?filename=test-%c4%99.html"},
		{"/ipns/?uri=ipns%3A%2F%2Fexample.com%2Fwiki%2FFoo_%C4%85%C4%99.html%3Ffilename%3Dtest-%C4%99.html", http.StatusMovedPermanently, "/ipns/example.com/wiki/Foo_%c4%85%c4%99.html?filename=test-%c4%99.html"},
		{"/ipns?uri=ipns://" + cid, http.StatusMovedPermanently, "/ipns/?uri=ipns://" + cid},
		{"/ipns/?uri=ipns://" + cid, http.StatusMovedPermanently, "/ipns/" + cid},
		{"/ipns/?uri=ipfs://" + cid, http.StatusMovedPermanently, "/ipfs/" + cid},
		{"/ipfs/?uri=unsupported://" + cid, http.StatusBadRequest, ""},
		{"/ipfs/?uri=invaliduri", http.StatusBadRequest, ""},
		{"/ipfs/?uri=" + cid, http.StatusBadRequest, ""},
	} {
		testName := ts.URL + test.path
		t.Run(testName, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, ts.URL+test.path, nil)
			assert.NoError(t, err)
			resp, err := doWithoutRedirect(r)
			assert.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, test.status, resp.StatusCode)
			assert.Equal(t, test.location, resp.Header.Get("Location"))
		})
	}
}

func TestIPNSHostnameRedirect(t *testing.T) {
	ts, api, root := newTestServerAndNode(t, nil)
	t.Logf("test server url: %s", ts.URL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	k, err := api.resolvePathNoRootsReturned(ctx, ipath.Join(ipath.IpfsPath(root), t.Name()))
	assert.NoError(t, err)

	t.Logf("k: %s\n", k)
	api.namesys["/ipns/example.net"] = path.FromString(k.String())

	// make request to directory containing index.html
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/foo", nil)
	assert.NoError(t, err)
	req.Host = "example.net"

	res, err := doWithoutRedirect(req)
	assert.NoError(t, err)

	// expect 301 redirect to same path, but with trailing slash
	assert.Equal(t, http.StatusMovedPermanently, res.StatusCode)
	hdr := res.Header["Location"]
	assert.Positive(t, len(hdr), "location header not present")
	assert.Equal(t, hdr[0], "/foo/")

	// make request with prefix to directory containing index.html
	req, err = http.NewRequest(http.MethodGet, ts.URL+"/foo", nil)
	assert.NoError(t, err)
	req.Host = "example.net"

	res, err = doWithoutRedirect(req)
	assert.NoError(t, err)
	// expect 301 redirect to same path, but with prefix and trailing slash
	assert.Equal(t, http.StatusMovedPermanently, res.StatusCode)

	hdr = res.Header["Location"]
	assert.Positive(t, len(hdr), "location header not present")
	assert.Equal(t, hdr[0], "/foo/")

	// make sure /version isn't exposed
	req, err = http.NewRequest(http.MethodGet, ts.URL+"/version", nil)
	assert.NoError(t, err)
	req.Host = "example.net"

	res, err = doWithoutRedirect(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, res.StatusCode)
}

// Test directory listing on DNSLink website
// (scenario when Host header is the same as URL hostname)
// This is basic regression test: additional end-to-end tests
// can be found in test/sharness/t0115-gateway-dir-listing.sh
func TestIPNSHostnameBacklinks(t *testing.T) {
	ts, api, root := newTestServerAndNode(t, nil)
	t.Logf("test server url: %s", ts.URL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	k, err := api.resolvePathNoRootsReturned(ctx, ipath.Join(ipath.IpfsPath(root), t.Name()))
	assert.NoError(t, err)

	// create /ipns/example.net/foo/
	k2, err := api.resolvePathNoRootsReturned(ctx, ipath.Join(k, "foo? #<'"))
	assert.NoError(t, err)

	k3, err := api.resolvePathNoRootsReturned(ctx, ipath.Join(k, "foo? #<'/bar"))
	assert.NoError(t, err)

	t.Logf("k: %s\n", k)
	api.namesys["/ipns/example.net"] = path.FromString(k.String())

	// make request to directory listing
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/foo%3F%20%23%3C%27/", nil)
	assert.NoError(t, err)
	req.Host = "example.net"

	res, err := doWithoutRedirect(req)
	assert.NoError(t, err)

	// expect correct links
	body, err := io.ReadAll(res.Body)
	assert.NoError(t, err)
	s := string(body)
	t.Logf("body: %s\n", string(body))

	assert.True(t, matchPathOrBreadcrumbs(s, "/ipns/<a href=\"//example.net/\">example.net</a>/<a href=\"//example.net/foo%3F%20%23%3C%27\">foo? #&lt;&#39;</a>"), "expected a path in directory listing")
	// https://github.com/ipfs/dir-index-html/issues/42
	assert.Contains(t, s, "<a class=\"ipfs-hash\" translate=\"no\" href=\"https://cid.ipfs.tech/#", "expected links to cid.ipfs.tech in CID column when on DNSLink website")
	assert.Contains(t, s, "<a href=\"/foo%3F%20%23%3C%27/..\">", "expected backlink in directory listing")
	assert.Contains(t, s, "<a href=\"/foo%3F%20%23%3C%27/file.txt\">", "expected file in directory listing")
	assert.Contains(t, s, s, k2.Cid().String(), "expected hash in directory listing")

	// make request to directory listing at root
	req, err = http.NewRequest(http.MethodGet, ts.URL, nil)
	assert.NoError(t, err)
	req.Host = "example.net"

	res, err = doWithoutRedirect(req)
	assert.NoError(t, err)

	// expect correct backlinks at root
	body, err = io.ReadAll(res.Body)
	assert.NoError(t, err)

	s = string(body)
	t.Logf("body: %s\n", string(body))

	assert.True(t, matchPathOrBreadcrumbs(s, "/"), "expected a path in directory listing")
	assert.NotContains(t, s, "<a href=\"/\">", "expected no backlink in directory listing of the root CID")
	assert.Contains(t, s, "<a href=\"/file.txt\">", "expected file in directory listing")
	// https://github.com/ipfs/dir-index-html/issues/42
	assert.Contains(t, s, "<a class=\"ipfs-hash\" translate=\"no\" href=\"https://cid.ipfs.tech/#", "expected links to cid.ipfs.tech in CID column when on DNSLink website")
	assert.Contains(t, s, k.Cid().String(), "expected hash in directory listing")

	// make request to directory listing
	req, err = http.NewRequest(http.MethodGet, ts.URL+"/foo%3F%20%23%3C%27/bar/", nil)
	assert.NoError(t, err)
	req.Host = "example.net"

	res, err = doWithoutRedirect(req)
	assert.NoError(t, err)

	// expect correct backlinks
	body, err = io.ReadAll(res.Body)
	assert.NoError(t, err)

	s = string(body)
	t.Logf("body: %s\n", string(body))

	assert.True(t, matchPathOrBreadcrumbs(s, "/ipns/<a href=\"//example.net/\">example.net</a>/<a href=\"//example.net/foo%3F%20%23%3C%27\">foo? #&lt;&#39;</a>/<a href=\"//example.net/foo%3F%20%23%3C%27/bar\">bar</a>"), "expected a path in directory listing")
	assert.Contains(t, s, "<a href=\"/foo%3F%20%23%3C%27/bar/..\">", "expected backlink in directory listing")
	assert.Contains(t, s, "<a href=\"/foo%3F%20%23%3C%27/bar/file.txt\">", "expected file in directory listing")
	assert.Contains(t, s, k3.Cid().String(), "expected hash in directory listing")
}

func TestPretty404(t *testing.T) {
	ts, api, root := newTestServerAndNode(t, nil)
	t.Logf("test server url: %s", ts.URL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	k, err := api.resolvePathNoRootsReturned(ctx, ipath.Join(ipath.IpfsPath(root), t.Name()))
	assert.NoError(t, err)

	host := "example.net"
	api.namesys["/ipns/"+host] = path.FromString(k.String())

	for _, test := range []struct {
		path   string
		accept string
		status int
		text   string
	}{
		{"/ipfs-404.html", "text/html", http.StatusOK, "Custom 404"},
		{"/nope", "text/html", http.StatusNotFound, "Custom 404"},
		{"/nope", "text/*", http.StatusNotFound, "Custom 404"},
		{"/nope", "*/*", http.StatusNotFound, "Custom 404"},
		{"/nope", "application/json", http.StatusNotFound, fmt.Sprintf("failed to resolve /ipns/example.net/nope: no link named \"nope\" under %s\n", k.Cid().String())},
		{"/deeper/nope", "text/html", http.StatusNotFound, "Deep custom 404"},
		{"/deeper/", "text/html", http.StatusOK, ""},
		{"/deeper", "text/html", http.StatusOK, ""},
		{"/nope/nope", "text/html", http.StatusNotFound, "Custom 404"},
	} {
		testName := fmt.Sprintf("%s %s", test.path, test.accept)
		t.Run(testName, func(t *testing.T) {
			var c http.Client
			req, err := http.NewRequest("GET", ts.URL+test.path, nil)
			assert.NoError(t, err)
			req.Header.Add("Accept", test.accept)
			req.Host = host
			resp, err := c.Do(req)
			assert.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, test.status, resp.StatusCode)
			body, err := io.ReadAll(resp.Body)
			assert.NoError(t, err)
			if test.text != "" {
				assert.Equal(t, test.text, string(body))
			}
		})
	}
}

func TestBrowserErrorHTML(t *testing.T) {
	ts, _, root := newTestServerAndNode(t, nil)
	t.Logf("test server url: %s", ts.URL)

	t.Run("plain error if request does not have Accept: text/html", func(t *testing.T) {
		t.Parallel()

		req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipfs/"+root.String()+"/nonexisting-link", nil)
		assert.Nil(t, err)

		res, err := doWithoutRedirect(req)
		assert.Nil(t, err)
		assert.Equal(t, http.StatusNotFound, res.StatusCode)
		assert.NotContains(t, res.Header.Get("Content-Type"), "text/html")

		body, err := io.ReadAll(res.Body)
		assert.Nil(t, err)
		assert.NotContains(t, string(body), "<!DOCTYPE html>")
	})

	t.Run("html error if request has Accept: text/html", func(t *testing.T) {
		t.Parallel()

		req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipfs/"+root.String()+"/nonexisting-link", nil)
		assert.Nil(t, err)
		req.Header.Set("Accept", "text/html")

		res, err := doWithoutRedirect(req)
		assert.Nil(t, err)
		assert.Equal(t, http.StatusNotFound, res.StatusCode)
		assert.Contains(t, res.Header.Get("Content-Type"), "text/html")

		body, err := io.ReadAll(res.Body)
		assert.Nil(t, err)
		assert.Contains(t, string(body), "<!DOCTYPE html>")
	})
}

func TestCacheControlImmutable(t *testing.T) {
	ts, _, root := newTestServerAndNode(t, nil)
	t.Logf("test server url: %s", ts.URL)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipfs/"+root.String()+"/", nil)
	assert.NoError(t, err)

	res, err := doWithoutRedirect(req)
	assert.NoError(t, err)

	// check the immutable tag isn't set
	hdrs, ok := res.Header["Cache-Control"]
	if ok {
		for _, hdr := range hdrs {
			assert.NotContains(t, hdr, "immutable", "unexpected Cache-Control: immutable on directory listing")
		}
	}
}

func TestGoGetSupport(t *testing.T) {
	ts, _, root := newTestServerAndNode(t, nil)
	t.Logf("test server url: %s", ts.URL)

	// mimic go-get
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipfs/"+root.String()+"?go-get=1", nil)
	assert.NoError(t, err)

	res, err := doWithoutRedirect(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)
}

func TestIpnsBase58MultihashRedirect(t *testing.T) {
	ts, _, _ := newTestServerAndNode(t, nil)
	t.Logf("test server url: %s", ts.URL)

	t.Run("ED25519 Base58-encoded key", func(t *testing.T) {
		t.Parallel()

		req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipns/12D3KooWRBy97UB99e3J6hiPesre1MZeuNQvfan4gBziswrRJsNK?keep=query", nil)
		assert.Nil(t, err)

		res, err := doWithoutRedirect(req)
		assert.Nil(t, err)
		assert.Equal(t, "/ipns/k51qzi5uqu5dlvj2baxnqndepeb86cbk3ng7n3i46uzyxzyqj2xjonzllnv0v8?keep=query", res.Header.Get("Location"))
	})

	t.Run("RSA Base58-encoded key", func(t *testing.T) {
		t.Parallel()

		req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipns/QmcJM7PRfkSbcM5cf1QugM5R37TLRKyJGgBEhXjLTB8uA2?keep=query", nil)
		assert.Nil(t, err)

		res, err := doWithoutRedirect(req)
		assert.Nil(t, err)
		assert.Equal(t, "/ipns/k2k4r8ol4m8kkcqz509c1rcjwunebj02gcnm5excpx842u736nja8ger?keep=query", res.Header.Get("Location"))
	})
}

func TestIpfsTrustlessMode(t *testing.T) {
	api, root := newMockAPI(t)

	ts := newTestServerWithConfig(t, api, Config{
		Headers:   map[string][]string{},
		NoDNSLink: false,
		PublicGateways: map[string]*Specification{
			"trustless.com": {
				Paths: []string{"/ipfs", "/ipns"},
			},
			"trusted.com": {
				Paths:                 []string{"/ipfs", "/ipns"},
				DeserializedResponses: true,
			},
		},
	})
	t.Logf("test server url: %s", ts.URL)

	trustedFormats := []string{"", "dag-json", "dag-cbor", "tar", "json", "cbor"}
	trustlessFormats := []string{"raw", "car"}

	doRequest := func(t *testing.T, path, host string, expectedStatus int) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+path, nil)
		assert.Nil(t, err)

		if host != "" {
			req.Host = host
		}

		res, err := doWithoutRedirect(req)
		assert.Nil(t, err)
		defer res.Body.Close()
		assert.Equal(t, expectedStatus, res.StatusCode)
	}

	doIpfsCidRequests := func(t *testing.T, formats []string, host string, expectedStatus int) {
		for _, format := range formats {
			doRequest(t, "/ipfs/"+root.String()+"/?format="+format, host, expectedStatus)
		}
	}

	doIpfsCidPathRequests := func(t *testing.T, formats []string, host string, expectedStatus int) {
		for _, format := range formats {
			doRequest(t, "/ipfs/"+root.String()+"/EmptyDir/?format="+format, host, expectedStatus)
		}
	}

	trustedTests := func(t *testing.T, host string) {
		doIpfsCidRequests(t, trustlessFormats, host, http.StatusOK)
		doIpfsCidRequests(t, trustedFormats, host, http.StatusOK)
		doIpfsCidPathRequests(t, trustlessFormats, host, http.StatusOK)
		doIpfsCidPathRequests(t, trustedFormats, host, http.StatusOK)
	}

	trustlessTests := func(t *testing.T, host string) {
		doIpfsCidRequests(t, trustlessFormats, host, http.StatusOK)
		doIpfsCidRequests(t, trustedFormats, host, http.StatusNotAcceptable)
		doIpfsCidPathRequests(t, trustlessFormats, host, http.StatusNotAcceptable)
		doIpfsCidPathRequests(t, trustedFormats, host, http.StatusNotAcceptable)
	}

	t.Run("Explicit Trustless Gateway", func(t *testing.T) {
		t.Parallel()
		trustlessTests(t, "trustless.com")
	})

	t.Run("Explicit Trusted Gateway", func(t *testing.T) {
		t.Parallel()
		trustedTests(t, "trusted.com")
	})

	t.Run("Implicit Default Trustless Gateway", func(t *testing.T) {
		t.Parallel()
		trustlessTests(t, "not.configured.com")
		trustlessTests(t, "localhost")
		trustlessTests(t, "127.0.0.1")
		trustlessTests(t, "::1")
	})
}

func TestIpnsTrustlessMode(t *testing.T) {
	api, root := newMockAPI(t)
	api.namesys["/ipns/trustless.com"] = path.FromCid(root)
	api.namesys["/ipns/trusted.com"] = path.FromCid(root)

	ts := newTestServerWithConfig(t, api, Config{
		Headers:   map[string][]string{},
		NoDNSLink: false,
		PublicGateways: map[string]*Specification{
			"trustless.com": {
				Paths: []string{"/ipfs", "/ipns"},
			},
			"trusted.com": {
				Paths:                 []string{"/ipfs", "/ipns"},
				DeserializedResponses: true,
			},
		},
	})
	t.Logf("test server url: %s", ts.URL)

	doRequest := func(t *testing.T, path, host string, expectedStatus int) {
		req, err := http.NewRequest(http.MethodGet, ts.URL+path, nil)
		assert.Nil(t, err)

		if host != "" {
			req.Host = host
		}

		res, err := doWithoutRedirect(req)
		assert.Nil(t, err)
		defer res.Body.Close()
		assert.Equal(t, expectedStatus, res.StatusCode)
	}

	// DNSLink only. Not supported for trustless. Supported for trusted, except
	// format=ipns-record which is unavailable for DNSLink.
	doRequest(t, "/", "trustless.com", http.StatusNotAcceptable)
	doRequest(t, "/EmptyDir/", "trustless.com", http.StatusNotAcceptable)
	doRequest(t, "/?format=ipns-record", "trustless.com", http.StatusNotAcceptable)

	doRequest(t, "/", "trusted.com", http.StatusOK)
	doRequest(t, "/EmptyDir/", "trusted.com", http.StatusOK)
	doRequest(t, "/?format=ipns-record", "trusted.com", http.StatusBadRequest)
}
