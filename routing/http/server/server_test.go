package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	b58 "github.com/mr-tron/base58/base58"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestHeaders(t *testing.T) {
	router := &mockContentRouter{}
	server := httptest.NewServer(Handler(router))
	t.Cleanup(server.Close)
	serverAddr := "http://" + server.Listener.Addr().String()

	results := iter.FromSlice([]iter.Result[types.Record]{
		{Val: &types.PeerRecord{
			Schema:    types.SchemaPeer,
			Protocols: []string{"transport-bitswap"},
		}}},
	)

	c := "baeabep4vu3ceru7nerjjbk37sxb7wmftteve4hcosmyolsbsiubw2vr6pqzj6mw7kv6tbn6nqkkldnklbjgm5tzbi4hkpkled4xlcr7xz4bq"
	cb, err := cid.Decode(c)
	require.NoError(t, err)

	router.On("FindProviders", mock.Anything, cb, DefaultRecordsLimit).
		Return(results, nil)

	resp, err := http.Get(serverAddr + "/routing/v1/providers/" + c)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	header := resp.Header.Get("Content-Type")
	require.Equal(t, mediaTypeJSON, header)

	resp, err = http.Get(serverAddr + "/routing/v1/providers/" + "BAD_CID")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 400, resp.StatusCode)
	header = resp.Header.Get("Content-Type")
	require.Equal(t, "text/plain; charset=utf-8", header)
}

func makePeerID(t *testing.T) (crypto.PrivKey, peer.ID) {
	sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	pid, err := peer.IDFromPrivateKey(sk)
	require.NoError(t, err)

	return sk, pid
}

func TestProviders(t *testing.T) {
	pidStr := "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn"
	pid2Str := "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vz"
	cidStr := "bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4"

	pid, err := peer.Decode(pidStr)
	require.NoError(t, err)
	pid2, err := peer.Decode(pid2Str)
	require.NoError(t, err)

	cid, err := cid.Decode(cidStr)
	require.NoError(t, err)

	runTest := func(t *testing.T, contentType string, expectedStream bool, expectedBody string) {
		t.Parallel()

		results := iter.FromSlice([]iter.Result[types.Record]{
			{Val: &types.PeerRecord{
				Schema:    types.SchemaPeer,
				ID:        &pid,
				Protocols: []string{"transport-bitswap"},
				Addrs:     []types.Multiaddr{},
			}},
			//lint:ignore SA1019 // ignore staticcheck
			{Val: &types.BitswapRecord{
				//lint:ignore SA1019 // ignore staticcheck
				Schema:   types.SchemaBitswap,
				ID:       &pid2,
				Protocol: "transport-bitswap",
				Addrs:    []types.Multiaddr{},
			}}},
		)

		router := &mockContentRouter{}
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)
		serverAddr := "http://" + server.Listener.Addr().String()
		limit := DefaultRecordsLimit
		if expectedStream {
			limit = DefaultStreamingRecordsLimit
		}
		router.On("FindProviders", mock.Anything, cid, limit).Return(results, nil)
		urlStr := serverAddr + "/routing/v1/providers/" + cidStr

		req, err := http.NewRequest(http.MethodGet, urlStr, nil)
		require.NoError(t, err)
		req.Header.Set("Accept", contentType)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		header := resp.Header.Get("Content-Type")
		require.Equal(t, contentType, header)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		require.Equal(t, expectedBody, string(body))
	}

	t.Run("JSON Response", func(t *testing.T) {
		runTest(t, mediaTypeJSON, false, `{"Providers":[{"Addrs":[],"ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn","Protocols":["transport-bitswap"],"Schema":"peer"},{"Schema":"bitswap","Protocol":"transport-bitswap","ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vz"}]}`)
	})

	t.Run("NDJSON Response", func(t *testing.T) {
		runTest(t, mediaTypeNDJSON, true, `{"Addrs":[],"ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn","Protocols":["transport-bitswap"],"Schema":"peer"}`+"\n"+`{"Schema":"bitswap","Protocol":"transport-bitswap","ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vz"}`+"\n")
	})
}

func TestPeers(t *testing.T) {
	makeRequest := func(t *testing.T, router *mockContentRouter, contentType, arg string) *http.Response {
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)
		req, err := http.NewRequest(http.MethodGet, "http://"+server.Listener.Addr().String()+"/routing/v1/peers/"+arg, nil)
		require.NoError(t, err)
		req.Header.Set("Accept", contentType)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		return resp
	}

	t.Run("GET /routing/v1/peers/{non-peer-cid} returns 400", func(t *testing.T) {
		t.Parallel()

		router := &mockContentRouter{}
		resp := makeRequest(t, router, mediaTypeJSON, "bafkqaaa")
		require.Equal(t, 400, resp.StatusCode)
	})

	t.Run("GET /routing/v1/peers/{base58-peer-id} returns 400", func(t *testing.T) {
		t.Parallel()

		_, pid := makePeerID(t)
		router := &mockContentRouter{}
		resp := makeRequest(t, router, mediaTypeJSON, b58.Encode([]byte(pid)))
		require.Equal(t, 400, resp.StatusCode)
	})

	t.Run("GET /routing/v1/peers/{cid-peer-id} returns 200 with correct body (JSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makePeerID(t)
		results := iter.FromSlice([]iter.Result[types.Record]{
			{Val: &types.PeerRecord{
				Schema:    types.SchemaPeer,
				ID:        &pid,
				Protocols: []string{"transport-bitswap", "transport-foo"},
				Addrs:     []types.Multiaddr{},
			}},
			{Val: &types.PeerRecord{
				Schema:    types.SchemaPeer,
				ID:        &pid,
				Protocols: []string{"transport-foo"},
				Addrs:     []types.Multiaddr{},
			}},
		})

		router := &mockContentRouter{}
		router.On("FindPeers", mock.Anything, pid, 20).Return(results, nil)

		resp := makeRequest(t, router, mediaTypeJSON, peer.ToCid(pid).String())
		require.Equal(t, 200, resp.StatusCode)

		header := resp.Header.Get("Content-Type")
		require.Equal(t, mediaTypeJSON, header)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedBody := `{"Peers":[{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-bitswap","transport-foo"],"Schema":"peer"},{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-foo"],"Schema":"peer"}]}`
		require.Equal(t, expectedBody, string(body))
	})

	t.Run("GET /routing/v1/peers/{cid-peer-id} returns 200 with correct body (NDJSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makePeerID(t)
		results := iter.FromSlice([]iter.Result[types.Record]{
			{Val: &types.PeerRecord{
				Schema:    types.SchemaPeer,
				ID:        &pid,
				Protocols: []string{"transport-bitswap", "transport-foo"},
				Addrs:     []types.Multiaddr{},
			}},
			{Val: &types.PeerRecord{
				Schema:    types.SchemaPeer,
				ID:        &pid,
				Protocols: []string{"transport-foo"},
				Addrs:     []types.Multiaddr{},
			}},
		})

		router := &mockContentRouter{}
		router.On("FindPeers", mock.Anything, pid, 0).Return(results, nil)

		resp := makeRequest(t, router, mediaTypeNDJSON, peer.ToCid(pid).String())
		require.Equal(t, 200, resp.StatusCode)

		header := resp.Header.Get("Content-Type")
		require.Equal(t, mediaTypeNDJSON, header)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedBody := `{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-bitswap","transport-foo"],"Schema":"peer"}` + "\n" + `{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-foo"],"Schema":"peer"}` + "\n"
		require.Equal(t, expectedBody, string(body))
	})
}

func makeName(t *testing.T) (crypto.PrivKey, ipns.Name) {
	sk, pid := makePeerID(t)
	return sk, ipns.NameFromPeer(pid)
}

func makeIPNSRecord(t *testing.T, cid cid.Cid, sk crypto.PrivKey, opts ...ipns.Option) (*ipns.Record, []byte) {
	path := path.NewIPFSPath(cid)
	eol := time.Now().Add(time.Hour * 48)
	ttl := time.Second * 20

	record, err := ipns.NewRecord(sk, path, 1, eol, ttl, opts...)
	require.NoError(t, err)

	rawRecord, err := ipns.MarshalRecord(record)
	require.NoError(t, err)

	return record, rawRecord
}

func TestIPNS(t *testing.T) {
	cid1, err := cid.Decode("bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	require.NoError(t, err)

	makeRequest := func(t *testing.T, router *mockContentRouter, path string) *http.Response {
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)
		serverAddr := "http://" + server.Listener.Addr().String()
		urlStr := serverAddr + path
		req, err := http.NewRequest(http.MethodGet, urlStr, nil)
		require.NoError(t, err)
		req.Header.Set("Accept", mediaTypeIPNSRecord)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		return resp
	}

	runWithRecordOptions := func(t *testing.T, opts ...ipns.Option) {
		sk, name1 := makeName(t)
		record1, rawRecord1 := makeIPNSRecord(t, cid1, sk)

		_, name2 := makeName(t)

		t.Run("GET /routing/v1/ipns/{cid-peer-id} returns 200", func(t *testing.T) {
			t.Parallel()

			rec, err := ipns.UnmarshalRecord(rawRecord1)
			require.NoError(t, err)

			router := &mockContentRouter{}
			router.On("GetIPNS", mock.Anything, name1).Return(rec, nil)

			resp := makeRequest(t, router, "/routing/v1/ipns/"+name1.String())
			require.Equal(t, 200, resp.StatusCode)
			require.Equal(t, mediaTypeIPNSRecord, resp.Header.Get("Content-Type"))
			require.NotEmpty(t, resp.Header.Get("Etag"))
			require.Equal(t, "max-age=20", resp.Header.Get("Cache-Control"))

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, body, rawRecord1)
		})

		t.Run("GET /routing/v1/ipns/{non-peer-cid} returns 400", func(t *testing.T) {
			t.Parallel()

			router := &mockContentRouter{}
			resp := makeRequest(t, router, "/routing/v1/ipns/"+cid1.String())
			require.Equal(t, 400, resp.StatusCode)
		})

		t.Run("GET /routing/v1/ipns/{peer-id} returns 400", func(t *testing.T) {
			t.Parallel()

			router := &mockContentRouter{}
			resp := makeRequest(t, router, "/routing/v1/ipns/"+name1.Peer().String())
			require.Equal(t, 400, resp.StatusCode)
		})

		t.Run("PUT /routing/v1/ipns/{cid-peer-id} returns 200", func(t *testing.T) {
			t.Parallel()

			router := &mockContentRouter{}
			router.On("PutIPNS", mock.Anything, name1, record1).Return(nil)

			server := httptest.NewServer(Handler(router))
			t.Cleanup(server.Close)
			serverAddr := "http://" + server.Listener.Addr().String()
			urlStr := serverAddr + "/routing/v1/ipns/" + name1.String()

			req, err := http.NewRequest(http.MethodPut, urlStr, bytes.NewReader(rawRecord1))
			require.NoError(t, err)
			req.Header.Set("Content-Type", mediaTypeIPNSRecord)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, 200, resp.StatusCode)
		})

		t.Run("PUT /routing/v1/ipns/{cid-peer-id} returns 400 for wrong record", func(t *testing.T) {
			t.Parallel()

			router := &mockContentRouter{}

			server := httptest.NewServer(Handler(router))
			t.Cleanup(server.Close)
			serverAddr := "http://" + server.Listener.Addr().String()
			urlStr := serverAddr + "/routing/v1/ipns/" + name2.String()

			req, err := http.NewRequest(http.MethodPut, urlStr, bytes.NewReader(rawRecord1))
			require.NoError(t, err)
			req.Header.Set("Content-Type", mediaTypeIPNSRecord)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, 400, resp.StatusCode)
		})
	}

	t.Run("V1+V2 IPNS Records", func(t *testing.T) {
		runWithRecordOptions(t, ipns.WithV1Compatibility(true))
	})

	t.Run("V2 IPNS Records", func(t *testing.T) {
		runWithRecordOptions(t, ipns.WithV1Compatibility(false))
	})
}

type mockContentRouter struct{ mock.Mock }

func (m *mockContentRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	args := m.Called(ctx, key, limit)
	return args.Get(0).(iter.ResultIter[types.Record]), args.Error(1)
}

func (m *mockContentRouter) ProvideBitswap(ctx context.Context, req *BitswapWriteProvideRequest) (time.Duration, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(time.Duration), args.Error(1)
}

func (m *mockContentRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[types.Record], error) {
	args := m.Called(ctx, pid, limit)
	return args.Get(0).(iter.ResultIter[types.Record]), args.Error(1)
}

func (m *mockContentRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	args := m.Called(ctx, name)
	return args.Get(0).(*ipns.Record), args.Error(1)
}

func (m *mockContentRouter) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	args := m.Called(ctx, name, record)
	return args.Error(0)
}
