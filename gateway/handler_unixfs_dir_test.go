package gateway

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/ipfs/boxo/path"
	"github.com/stretchr/testify/require"
)

func TestIPNSHostnameBacklinks(t *testing.T) {
	// Test if directory listing on DNSLink Websites have correct backlinks.
	ts, backend, root := newTestServerAndNode(t, nil, "dir-special-chars.car")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create /ipns/example.net/foo/
	p2, err := path.Join(path.FromCid(root), "foo? #<'")
	require.NoError(t, err)
	k2, err := backend.resolvePathNoRootsReturned(ctx, p2)
	require.NoError(t, err)

	p3, err := path.Join(path.FromCid(root), "foo? #<'/bar")
	require.NoError(t, err)
	k3, err := backend.resolvePathNoRootsReturned(ctx, p3)
	require.NoError(t, err)

	backend.namesys["/ipns/example.net"] = newMockNamesysItem(path.FromCid(root), 0)

	// make request to directory listing
	req := mustNewRequest(t, http.MethodGet, ts.URL+"/foo%3F%20%23%3C%27/", nil)
	req.Host = "example.net"

	res := mustDoWithoutRedirect(t, req)

	// expect correct links
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	s := string(body)
	t.Logf("body: %s\n", string(body))

	require.True(t, matchPathOrBreadcrumbs(s, "/ipns/<a href=\"//example.net/\">example.net</a>/<a href=\"//example.net/foo%3F%20%23%3C%27\">foo? #&lt;&#39;</a>"), "expected a path in directory listing")
	// https://github.com/ipfs/dir-index-html/issues/42
	require.Contains(t, s, "<a class=\"ipfs-hash\" translate=\"no\" href=\"https://cid.ipfs.tech/#", "expected links to cid.ipfs.tech in CID column when on DNSLink website")
	require.Contains(t, s, "<a href=\"/foo%3F%20%23%3C%27/..\">", "expected backlink in directory listing")
	require.Contains(t, s, "<a href=\"/foo%3F%20%23%3C%27/file.txt\">", "expected file in directory listing")
	require.Contains(t, s, s, k2.RootCid().String(), "expected hash in directory listing")

	// make request to directory listing at root
	req = mustNewRequest(t, http.MethodGet, ts.URL, nil)
	req.Host = "example.net"

	res = mustDoWithoutRedirect(t, req)
	require.NoError(t, err)

	// expect correct backlinks at root
	body, err = io.ReadAll(res.Body)
	require.NoError(t, err)

	s = string(body)
	t.Logf("body: %s\n", string(body))

	require.True(t, matchPathOrBreadcrumbs(s, "/"), "expected a path in directory listing")
	require.NotContains(t, s, "<a href=\"/\">", "expected no backlink in directory listing of the root CID")
	require.Contains(t, s, "<a href=\"/file.txt\">", "expected file in directory listing")
	// https://github.com/ipfs/dir-index-html/issues/42
	require.Contains(t, s, "<a class=\"ipfs-hash\" translate=\"no\" href=\"https://cid.ipfs.tech/#", "expected links to cid.ipfs.tech in CID column when on DNSLink website")
	require.Contains(t, s, root.String(), "expected hash in directory listing")

	// make request to directory listing
	req = mustNewRequest(t, http.MethodGet, ts.URL+"/foo%3F%20%23%3C%27/bar/", nil)
	req.Host = "example.net"

	res = mustDoWithoutRedirect(t, req)

	// expect correct backlinks
	body, err = io.ReadAll(res.Body)
	require.NoError(t, err)

	s = string(body)
	t.Logf("body: %s\n", string(body))

	require.True(t, matchPathOrBreadcrumbs(s, "/ipns/<a href=\"//example.net/\">example.net</a>/<a href=\"//example.net/foo%3F%20%23%3C%27\">foo? #&lt;&#39;</a>/<a href=\"//example.net/foo%3F%20%23%3C%27/bar\">bar</a>"), "expected a path in directory listing")
	require.Contains(t, s, "<a href=\"/foo%3F%20%23%3C%27/bar/..\">", "expected backlink in directory listing")
	require.Contains(t, s, "<a href=\"/foo%3F%20%23%3C%27/bar/file.txt\">", "expected file in directory listing")
	require.Contains(t, s, k3.RootCid().String(), "expected hash in directory listing")
}
