package gateway

import (
	"context"
	"html"
	"io"
	"net/http"
	"testing"

	ipath "github.com/ipfs/boxo/coreiface/path"
	"github.com/stretchr/testify/require"
)

func TestDagJsonCborPreview(t *testing.T) {
	t.Parallel()
	backend, root := newMockBackend(t, "fixtures.car")

	ts := newTestServerWithConfig(t, backend, Config{
		Headers:   map[string][]string{},
		NoDNSLink: false,
		PublicGateways: map[string]*PublicGateway{
			"example.com": {
				Paths:                 []string{"/ipfs", "/ipns"},
				UseSubdomains:         true,
				DeserializedResponses: true,
			},
		},
		DeserializedResponses: true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resolvedPath, err := backend.resolvePathNoRootsReturned(ctx, ipath.Join(ipath.IpfsPath(root), "subdir", "dag-cbor-document"))
	require.NoError(t, err)

	cidStr := resolvedPath.Cid().String()

	t.Run("path gateway normalizes to trailing slash", func(t *testing.T) {
		t.Parallel()

		req := mustNewRequest(t, http.MethodGet, ts.URL+"/ipfs/"+cidStr, nil)
		req.Header.Add("Accept", "text/html")

		res := mustDoWithoutRedirect(t, req)
		require.Equal(t, http.StatusMovedPermanently, res.StatusCode)
		require.Equal(t, "/ipfs/"+cidStr+"/", res.Header.Get("Location"))
	})

	t.Run("subdomain gateway correctly redirects", func(t *testing.T) {
		t.Parallel()

		req := mustNewRequest(t, http.MethodGet, ts.URL+"/ipfs/"+cidStr, nil)
		req.Header.Add("Accept", "text/html")
		req.Host = "example.com"

		res := mustDoWithoutRedirect(t, req)
		require.Equal(t, http.StatusMovedPermanently, res.StatusCode)
		require.Equal(t, "http://"+cidStr+".ipfs.example.com/", res.Header.Get("Location"))
	})

	t.Run("preview strings are correctly escaped", func(t *testing.T) {
		t.Parallel()

		req := mustNewRequest(t, http.MethodGet, ts.URL+resolvedPath.String()+"/", nil)
		req.Header.Add("Accept", "text/html")

		res := mustDoWithoutRedirect(t, req)
		require.Equal(t, http.StatusOK, res.StatusCode)

		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)

		script := "<string>window.alert('hacked')</string>"
		escaped := html.EscapeString(script)

		require.Contains(t, string(body), escaped)
		require.NotContains(t, string(body), script)
	})
}
