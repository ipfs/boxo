package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	gopath "path"
	"regexp"
	"strings"
	"testing"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	bsfetcher "github.com/ipfs/go-fetcher/impl/blockservice"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/boxo/blocks"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-namesys"
	"github.com/ipfs/go-namesys/resolve"
	path "github.com/ipfs/go-path"
	"github.com/ipfs/go-path/resolver"
	"github.com/ipfs/go-unixfs"
	ufile "github.com/ipfs/go-unixfs/file"
	uio "github.com/ipfs/go-unixfs/io"
	"github.com/ipfs/go-unixfsnode"
	iface "github.com/ipfs/interface-go-ipfs-core"
	nsopts "github.com/ipfs/interface-go-ipfs-core/options/namesys"
	ipath "github.com/ipfs/interface-go-ipfs-core/path"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
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
	blockStore   blockstore.Blockstore
	blockService blockservice.BlockService
	dagService   format.DAGService
	resolver     resolver.Resolver
	namesys      mockNamesys
}

func newMockAPI(t *testing.T) (*mockAPI, cid.Cid) {
	r, err := os.Open("./testdata/fixtures.car")
	assert.Nil(t, err)

	blockStore, err := carblockstore.NewReadOnly(r, nil)
	assert.Nil(t, err)

	t.Cleanup(func() {
		blockStore.Close()
		r.Close()
	})

	cids, err := blockStore.Roots()
	assert.Nil(t, err)
	assert.Len(t, cids, 1)

	blockService := blockservice.New(blockStore, offline.Exchange(blockStore))
	dagService := merkledag.NewDAGService(blockService)

	fetcherConfig := bsfetcher.NewFetcherConfig(blockService)
	fetcherConfig.PrototypeChooser = dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})
	fetcher := fetcherConfig.WithReifier(unixfsnode.Reify)
	resolver := resolver.NewBasicResolver(fetcher)

	return &mockAPI{
		blockStore:   blockService.Blockstore(),
		blockService: blockService,
		dagService:   dagService,
		resolver:     resolver,
		namesys:      mockNamesys{},
	}, cids[0]
}

func (api *mockAPI) GetUnixFsNode(ctx context.Context, p ipath.Resolved) (files.Node, error) {
	nd, err := api.resolveNode(ctx, p)
	if err != nil {
		return nil, err
	}

	return ufile.NewUnixfsFile(ctx, api.dagService, nd)
}

func (api *mockAPI) LsUnixFsDir(ctx context.Context, p ipath.Resolved) (<-chan iface.DirEntry, error) {
	node, err := api.resolveNode(ctx, p)
	if err != nil {
		return nil, err
	}

	dir, err := uio.NewDirectoryFromNode(api.dagService, node)
	if err != nil {
		return nil, err
	}

	out := make(chan iface.DirEntry, uio.DefaultShardWidth)

	go func() {
		defer close(out)
		for l := range dir.EnumLinksAsync(ctx) {
			select {
			case out <- api.processLink(ctx, l):
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

func (api *mockAPI) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return api.blockService.GetBlock(ctx, c)
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
	rp, err := api.ResolvePath(ctx, p)
	if err != nil {
		return false
	}

	has, _ := api.blockStore.Has(ctx, rp.Cid())
	return has
}

func (api *mockAPI) ResolvePath(ctx context.Context, ip ipath.Path) (ipath.Resolved, error) {
	if _, ok := ip.(ipath.Resolved); ok {
		return ip.(ipath.Resolved), nil
	}

	err := ip.IsValid()
	if err != nil {
		return nil, err
	}

	p := path.Path(ip.String())
	if p.Segments()[0] == "ipns" {
		p, err = resolve.ResolveIPNS(ctx, api.namesys, p)
		if err != nil {
			return nil, err
		}
	}

	if p.Segments()[0] != "ipfs" {
		return nil, fmt.Errorf("unsupported path namespace: %s", ip.Namespace())
	}

	node, rest, err := api.resolver.ResolveToLastNode(ctx, p)
	if err != nil {
		return nil, err
	}

	root, err := cid.Parse(p.Segments()[1])
	if err != nil {
		return nil, err
	}

	return ipath.NewResolvedPath(p, node, root, gopath.Join(rest...)), nil
}

func (api *mockAPI) resolveNode(ctx context.Context, p ipath.Path) (format.Node, error) {
	rp, err := api.ResolvePath(ctx, p)
	if err != nil {
		return nil, err
	}

	node, err := api.dagService.Get(ctx, rp.Cid())
	if err != nil {
		return nil, fmt.Errorf("get node: %w", err)
	}
	return node, nil
}

func (api *mockAPI) processLink(ctx context.Context, result unixfs.LinkResult) iface.DirEntry {
	if result.Err != nil {
		return iface.DirEntry{Err: result.Err}
	}

	link := iface.DirEntry{
		Name: result.Link.Name,
		Cid:  result.Link.Cid,
	}

	switch link.Cid.Type() {
	case cid.Raw:
		link.Type = iface.TFile
		link.Size = result.Link.Size
	case cid.DagProtobuf:
		link.Size = result.Link.Size
	}

	return link
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

func newTestServer(t *testing.T, api API) *httptest.Server {
	config := Config{Headers: map[string][]string{}}
	AddAccessControlHeaders(config.Headers)

	handler := NewHandler(config, api)
	mux := http.NewServeMux()
	mux.Handle("/ipfs/", handler)
	mux.Handle("/ipns/", handler)
	handler = WithHostname(mux, api, map[string]*Specification{}, false)

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

	k, err := api.ResolvePath(ctx, ipath.Join(ipath.IpfsPath(root), t.Name(), "fnord"))
	assert.Nil(t, err)

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
		{"127.0.0.1:8080", "/" + k.Cid().String(), http.StatusNotFound, "404 page not found\n"},
		{"127.0.0.1:8080", "/ipfs/this-is-not-a-cid", http.StatusBadRequest, "failed to resolve /ipfs/this-is-not-a-cid: invalid path \"/ipfs/this-is-not-a-cid\": invalid CID: invalid cid: illegal base32 data at input byte 3\n"},
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
			assert.Nil(t, err)
			r.Host = test.host
			resp, err := c.Do(r)
			assert.Nil(t, err)
			defer resp.Body.Close()
			assert.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("Content-Type"))
			body, err := io.ReadAll(resp.Body)
			assert.Nil(t, err)
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
			assert.Nil(t, err)
			resp, err := doWithoutRedirect(r)
			assert.Nil(t, err)
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

	k, err := api.ResolvePath(ctx, ipath.Join(ipath.IpfsPath(root), t.Name()))
	assert.Nil(t, err)

	t.Logf("k: %s\n", k)
	api.namesys["/ipns/example.net"] = path.FromString(k.String())

	// make request to directory containing index.html
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/foo", nil)
	assert.Nil(t, err)
	req.Host = "example.net"

	res, err := doWithoutRedirect(req)
	assert.Nil(t, err)

	// expect 301 redirect to same path, but with trailing slash
	assert.Equal(t, http.StatusMovedPermanently, res.StatusCode)
	hdr := res.Header["Location"]
	assert.Positive(t, len(hdr), "location header not present")
	assert.Equal(t, hdr[0], "/foo/")

	// make request with prefix to directory containing index.html
	req, err = http.NewRequest(http.MethodGet, ts.URL+"/foo", nil)
	assert.Nil(t, err)
	req.Host = "example.net"

	res, err = doWithoutRedirect(req)
	assert.Nil(t, err)
	// expect 301 redirect to same path, but with prefix and trailing slash
	assert.Equal(t, http.StatusMovedPermanently, res.StatusCode)

	hdr = res.Header["Location"]
	assert.Positive(t, len(hdr), "location header not present")
	assert.Equal(t, hdr[0], "/foo/")

	// make sure /version isn't exposed
	req, err = http.NewRequest(http.MethodGet, ts.URL+"/version", nil)
	assert.Nil(t, err)
	req.Host = "example.net"

	res, err = doWithoutRedirect(req)
	assert.Nil(t, err)
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

	k, err := api.ResolvePath(ctx, ipath.Join(ipath.IpfsPath(root), t.Name()))
	assert.Nil(t, err)

	// create /ipns/example.net/foo/
	k2, err := api.ResolvePath(ctx, ipath.Join(k, "foo? #<'"))
	assert.Nil(t, err)

	k3, err := api.ResolvePath(ctx, ipath.Join(k, "foo? #<'/bar"))
	assert.Nil(t, err)

	t.Logf("k: %s\n", k)
	api.namesys["/ipns/example.net"] = path.FromString(k.String())

	// make request to directory listing
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/foo%3F%20%23%3C%27/", nil)
	assert.Nil(t, err)
	req.Host = "example.net"

	res, err := doWithoutRedirect(req)
	assert.Nil(t, err)

	// expect correct links
	body, err := io.ReadAll(res.Body)
	assert.Nil(t, err)
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
	assert.Nil(t, err)
	req.Host = "example.net"

	res, err = doWithoutRedirect(req)
	assert.Nil(t, err)

	// expect correct backlinks at root
	body, err = io.ReadAll(res.Body)
	assert.Nil(t, err)

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
	assert.Nil(t, err)
	req.Host = "example.net"

	res, err = doWithoutRedirect(req)
	assert.Nil(t, err)

	// expect correct backlinks
	body, err = io.ReadAll(res.Body)
	assert.Nil(t, err)

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

	k, err := api.ResolvePath(ctx, ipath.Join(ipath.IpfsPath(root), t.Name()))
	assert.Nil(t, err)

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
			assert.Nil(t, err)
			req.Header.Add("Accept", test.accept)
			req.Host = host
			resp, err := c.Do(req)
			assert.Nil(t, err)
			defer resp.Body.Close()
			assert.Equal(t, test.status, resp.StatusCode)
			body, err := io.ReadAll(resp.Body)
			assert.Nil(t, err)
			if test.text != "" {
				assert.Equal(t, test.text, string(body))
			}
		})
	}
}

func TestCacheControlImmutable(t *testing.T) {
	ts, _, root := newTestServerAndNode(t, nil)
	t.Logf("test server url: %s", ts.URL)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipfs/"+root.String()+"/", nil)
	assert.Nil(t, err)

	res, err := doWithoutRedirect(req)
	assert.Nil(t, err)

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
	assert.Nil(t, err)

	res, err := doWithoutRedirect(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)
}
