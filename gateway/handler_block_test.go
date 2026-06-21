package gateway

import (
	"io"
	"net/http"
	"testing"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// TestEmptyIdentityCIDProbeShortCircuit verifies that the trustless-gateway
// probe (GET/HEAD /ipfs/bafkqaaa?format=raw) returns 200 with an empty body even
// when the backend cannot reconstruct identity CIDs. The backend here is a plain
// blockstore (no [blockstore.NewIdStore]) with an offline exchange, so a 200
// proves the handler short-circuit answers the probe, not the backend.
func TestEmptyIdentityCIDProbeShortCircuit(t *testing.T) {
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	blockService := blockservice.New(bs, offline.Exchange(bs))
	backend, err := NewBlocksBackend(blockService, WithNameSystem(mockNamesys{}))
	require.NoError(t, err)

	// Sanity check: the backend really cannot serve the probe block by itself.
	_, _, err = backend.GetBlock(t.Context(), path.FromCid(EmptyIdentityCID))
	require.True(t, ipld.IsNotFound(err), "plain backend unexpectedly served the identity block: %v", err)

	ts := newTestServer(t, backend)

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			req := mustNewRequest(t, method, ts.URL+"/ipfs/"+EmptyIdentityCIDString+"?format=raw", nil)
			req.Header.Set("Accept", rawResponseFormat)
			res := mustDo(t, req)
			defer res.Body.Close()

			require.Equal(t, http.StatusOK, res.StatusCode)
			require.Equal(t, rawResponseFormat, res.Header.Get("Content-Type"))

			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			require.Empty(t, body)
		})
	}

	// The short-circuit is scoped to the empty identity CID only. A different
	// identity CID is left to the backend, which here cannot reconstruct it, so
	// the gateway must return 404 rather than an empty 200.
	t.Run("other identity CID is not short-circuited", func(t *testing.T) {
		otherHash, err := mh.Sum([]byte("not empty"), mh.IDENTITY, -1)
		require.NoError(t, err)
		otherCID := cid.NewCidV1(cid.Raw, otherHash)

		req := mustNewRequest(t, http.MethodGet, ts.URL+"/ipfs/"+otherCID.String()+"?format=raw", nil)
		req.Header.Set("Accept", rawResponseFormat)
		res := mustDo(t, req)
		defer res.Body.Close()
		require.Equal(t, http.StatusNotFound, res.StatusCode)
	})
}

// BenchmarkIsEmptyIdentityProbe guards that the check added to every raw-block
// request stays allocation-free for the common case (a CID that is not the
// probe), where the CID compare short-circuits before the allocating Segments()
// call.
func BenchmarkIsEmptyIdentityProbe(b *testing.B) {
	h, err := mh.Sum([]byte("a typical raw block"), mh.SHA2_256, -1)
	require.NoError(b, err)
	rq := &requestData{immutablePath: path.FromCid(cid.NewCidV1(cid.Raw, h))}

	b.ReportAllocs()
	for b.Loop() {
		if isEmptyIdentityProbe(rq) {
			b.Fatal("unexpected probe match")
		}
	}
}
