package gateway

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/files"
	iface "github.com/ipfs/interface-go-ipfs-core"
	ipath "github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/tj/assert"
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
		if result != test.expected {
			t.Fatalf("unexpected result of etagMatch(%q, %q, %q), got %t, expected %t", test.header, test.cidEtag, test.dirEtag, result, test.expected)
		}
	}
}

type errorMockAPI struct {
	err error
}

func (api *errorMockAPI) GetUnixFsNode(context.Context, ipath.Resolved) (files.Node, error) {
	return nil, api.err
}

func (api *errorMockAPI) LsUnixFsDir(ctx context.Context, p ipath.Resolved) (<-chan iface.DirEntry, error) {
	return nil, api.err
}

func (api *errorMockAPI) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return nil, api.err
}

func (api *errorMockAPI) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	return nil, api.err
}

func (api *errorMockAPI) GetDNSLinkRecord(ctx context.Context, hostname string) (ipath.Path, error) {
	return nil, api.err
}

func (api *errorMockAPI) IsCached(ctx context.Context, p ipath.Path) bool {
	return false
}

func (api *errorMockAPI) ResolvePath(ctx context.Context, ip ipath.Path) (ipath.Resolved, error) {
	return nil, api.err
}

func TestGatewayBadRequestInvalidPath(t *testing.T) {
	api, _ := newMockAPI(t)
	ts := newTestServer(t, api)
	t.Logf("test server url: %s", ts.URL)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipfs/QmInvalid/Path", nil)
	assert.Nil(t, err)

	res, err := ts.Client().Do(req)
	assert.Nil(t, err)

	assert.Equal(t, http.StatusBadRequest, res.StatusCode)
}

func TestGatewayTimeoutBubblingFromAPI(t *testing.T) {
	api := &errorMockAPI{err: fmt.Errorf("the mock api has timed out: %w", ErrGatewayTimeout)}
	ts := newTestServer(t, api)
	t.Logf("test server url: %s", ts.URL)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipns/en.wikipedia-on-ipfs.org", nil)
	assert.Nil(t, err)

	res, err := ts.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusGatewayTimeout, res.StatusCode)
}

func TestBadGatewayBubblingFromAPI(t *testing.T) {
	api := &errorMockAPI{err: fmt.Errorf("the mock api has a bad gateway: %w", ErrBadGateway)}
	ts := newTestServer(t, api)
	t.Logf("test server url: %s", ts.URL)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/ipns/en.wikipedia-on-ipfs.org", nil)
	assert.Nil(t, err)

	res, err := ts.Client().Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadGateway, res.StatusCode)
}
