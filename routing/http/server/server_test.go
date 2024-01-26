package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	tjson "github.com/ipfs/boxo/routing/http/types/json"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	b58 "github.com/mr-tron/base58/base58"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
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

func makeCID(t *testing.T) cid.Cid {
	buf := make([]byte, 63)
	_, err := rand.Read(buf)
	require.NoError(t, err)
	mh, err := multihash.Encode(buf, multihash.SHA2_256)
	require.NoError(t, err)
	c := cid.NewCidV1(0, mh)
	return c
}

func TestProviders(t *testing.T) {
	// Prepare some variables common to all tests.
	sk1, pid1 := makePeerID(t)
	pid1Str := pid1.String()

	sk2, pid2 := makePeerID(t)
	pid2Str := pid2.String()

	cid1 := makeCID(t)
	cid1Str := cid1.String()

	// GET Tests
	runGetTest := func(t *testing.T, contentType string, expectedStream bool, expectedBody string) {
		t.Parallel()

		results := iter.FromSlice([]iter.Result[types.Record]{
			{Val: &types.PeerRecord{
				Schema:    types.SchemaPeer,
				ID:        &pid1,
				Protocols: []string{"transport-bitswap"},
				Addrs:     []types.Multiaddr{},
			}},
			{Val: &types.PeerRecord{
				Schema:    types.SchemaPeer,
				ID:        &pid2,
				Protocols: []string{"transport-bitswap"},
				Addrs:     []types.Multiaddr{},
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
		router.On("FindProviders", mock.Anything, cid1, limit).Return(results, nil)
		urlStr := serverAddr + "/routing/v1/providers/" + cid1Str

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

	t.Run("GET /routing/v1/peers/{cid} (JSON Response)", func(t *testing.T) {
		runGetTest(t, mediaTypeJSON, false, `{"Providers":[{"Addrs":[],"ID":"`+pid1Str+`","Protocols":["transport-bitswap"],"Schema":"peer"},{"Addrs":[],"ID":"`+pid2Str+`","Protocols":["transport-bitswap"],"Schema":"peer"}]}`)
	})

	t.Run("GET /routing/v1/peers/{cid} (NDJSON Response)", func(t *testing.T) {
		runGetTest(t, mediaTypeNDJSON, true, `{"Addrs":[],"ID":"`+pid1Str+`","Protocols":["transport-bitswap"],"Schema":"peer"}`+"\n"+`{"Addrs":[],"ID":"`+pid2Str+`","Protocols":["transport-bitswap"],"Schema":"peer"}`+"\n")
	})

	runPutTest := func(t *testing.T, contentType string, expectedBody string) {
		t.Parallel()

		rec1 := &types.AnnouncementRecord{
			Schema: types.SchemaAnnouncement,
			Payload: types.AnnouncementPayload{
				CID:       cid1,
				Timestamp: time.Now().UTC(),
				TTL:       time.Hour,
				ID:        &pid1,
				Protocols: []string{"transport-🌈"},
			},
		}
		err := rec1.Sign(pid1, sk1)
		require.NoError(t, err)

		rec2 := &types.AnnouncementRecord{
			Schema: types.SchemaAnnouncement,
			Payload: types.AnnouncementPayload{
				CID:       cid1,
				Timestamp: time.Now().UTC(),
				TTL:       time.Hour,
				ID:        &pid2,
				Protocols: []string{"transport-🌈"},
			},
		}
		err = rec2.Sign(pid2, sk2)
		require.NoError(t, err)

		req := tjson.AnnounceProvidersRequest{Providers: []types.Record{rec1, rec2}}
		body, err := json.Marshal(req)
		require.NoError(t, err)

		router := &mockContentRouter{}
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)

		serverAddr := "http://" + server.Listener.Addr().String()

		router.On("Provide", mock.Anything, &ProvideRequest{
			CID:       cid1,
			Timestamp: rec1.Payload.Timestamp,
			TTL:       rec1.Payload.TTL,
			ID:        pid1,
			Addrs:     []multiaddr.Multiaddr{},
			Protocols: rec1.Payload.Protocols,
		}).Return(time.Hour, nil)

		router.On("Provide", mock.Anything, &ProvideRequest{
			CID:       cid1,
			Timestamp: rec2.Payload.Timestamp,
			TTL:       rec2.Payload.TTL,
			ID:        pid2,
			Addrs:     []multiaddr.Multiaddr{},
			Protocols: rec2.Payload.Protocols,
		}).Return(time.Minute, nil)

		urlStr := serverAddr + "/routing/v1/providers"

		httpReq, err := http.NewRequest(http.MethodPut, urlStr, bytes.NewReader(body))
		require.NoError(t, err)
		httpReq.Header.Set("Accept", contentType)

		resp, err := http.DefaultClient.Do(httpReq)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		header := resp.Header.Get("Content-Type")
		require.Equal(t, contentType, header)

		body, err = io.ReadAll(resp.Body)
		require.NoError(t, err)

		require.Equal(t, expectedBody, string(body))
	}

	t.Run("PUT /routing/v1/providers (JSON Response)", func(t *testing.T) {
		runPutTest(t, mediaTypeJSON, `{"ProvideResults":[{"Schema":"announcement","Payload":{"Addrs":[],"CID":"`+cid1Str+`","ID":"`+pid1Str+`","Protocols":[],"TTL":3600000}},{"Schema":"announcement","Payload":{"Addrs":[],"CID":"`+cid1Str+`","ID":"`+pid2Str+`","Protocols":[],"TTL":60000}}]}`)
	})

	t.Run("PUT /routing/v1/providers (NDJSON Response)", func(t *testing.T) {
		runPutTest(t, mediaTypeNDJSON, `{"Schema":"announcement","Payload":{"Addrs":[],"CID":"`+cid1Str+`","ID":"`+pid1Str+`","Protocols":[],"TTL":3600000}}`+"\n"+`{"Schema":"announcement","Payload":{"Addrs":[],"CID":"`+cid1Str+`","ID":"`+pid2Str+`","Protocols":[],"TTL":60000}}`+"\n")
	})
}

func TestPeers(t *testing.T) {
	makeGetRequest := func(t *testing.T, router *mockContentRouter, contentType, arg string) *http.Response {
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
		resp := makeGetRequest(t, router, mediaTypeJSON, "bafkqaaa")
		require.Equal(t, 400, resp.StatusCode)
	})

	t.Run("GET /routing/v1/peers/{base58-peer-id} returns 400", func(t *testing.T) {
		t.Parallel()

		_, pid := makePeerID(t)
		router := &mockContentRouter{}
		resp := makeGetRequest(t, router, mediaTypeJSON, b58.Encode([]byte(pid)))
		require.Equal(t, 400, resp.StatusCode)
	})

	t.Run("GET /routing/v1/peers/{cid-peer-id} returns 200 with correct body (JSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makePeerID(t)
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{
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

		resp := makeGetRequest(t, router, mediaTypeJSON, peer.ToCid(pid).String())
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
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{
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

		resp := makeGetRequest(t, router, mediaTypeNDJSON, peer.ToCid(pid).String())
		require.Equal(t, 200, resp.StatusCode)

		header := resp.Header.Get("Content-Type")
		require.Equal(t, mediaTypeNDJSON, header)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedBody := `{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-bitswap","transport-foo"],"Schema":"peer"}` + "\n" + `{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-foo"],"Schema":"peer"}` + "\n"
		require.Equal(t, expectedBody, string(body))
	})

	sk1, pid1 := makePeerID(t)
	sk2, pid2 := makePeerID(t)

	runPutTest := func(t *testing.T, contentType string, expectedBody string) {
		t.Parallel()

		rec1 := &types.AnnouncementRecord{
			Schema: types.SchemaAnnouncement,
			Payload: types.AnnouncementPayload{
				Timestamp: time.Now().UTC(),
				TTL:       time.Hour,
				ID:        &pid1,
				Protocols: []string{"transport-🌈"},
			},
		}
		err := rec1.Sign(pid1, sk1)
		require.NoError(t, err)

		rec2 := &types.AnnouncementRecord{
			Schema: types.SchemaAnnouncement,
			Payload: types.AnnouncementPayload{
				Timestamp: time.Now().UTC(),
				TTL:       time.Hour,
				ID:        &pid2,
				Protocols: []string{"transport-🌈"},
			},
		}
		err = rec2.Sign(pid2, sk2)
		require.NoError(t, err)

		req := tjson.AnnounceProvidersRequest{Providers: []types.Record{rec1, rec2}}
		body, err := json.Marshal(req)
		require.NoError(t, err)

		router := &mockContentRouter{}
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)

		serverAddr := "http://" + server.Listener.Addr().String()

		router.On("ProvidePeer", mock.Anything, &ProvidePeerRequest{
			Timestamp: rec1.Payload.Timestamp,
			TTL:       rec1.Payload.TTL,
			ID:        pid1,
			Addrs:     []multiaddr.Multiaddr{},
			Protocols: rec1.Payload.Protocols,
		}).Return(time.Hour, nil)

		router.On("ProvidePeer", mock.Anything, &ProvidePeerRequest{
			Timestamp: rec2.Payload.Timestamp,
			TTL:       rec2.Payload.TTL,
			ID:        pid2,
			Addrs:     []multiaddr.Multiaddr{},
			Protocols: rec2.Payload.Protocols,
		}).Return(time.Minute, nil)

		urlStr := serverAddr + "/routing/v1/peers"

		httpReq, err := http.NewRequest(http.MethodPut, urlStr, bytes.NewReader(body))
		require.NoError(t, err)
		httpReq.Header.Set("Accept", contentType)

		resp, err := http.DefaultClient.Do(httpReq)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		header := resp.Header.Get("Content-Type")
		require.Equal(t, contentType, header)

		body, err = io.ReadAll(resp.Body)
		require.NoError(t, err)

		require.Equal(t, expectedBody, string(body))
	}

	t.Run("PUT /routing/v1/peers (JSON Response)", func(t *testing.T) {
		runPutTest(t, mediaTypeJSON, `{"ProvideResults":[{"Schema":"announcement","Payload":{"Addrs":[],"ID":"`+pid1.String()+`","Protocols":[],"TTL":3600000}},{"Schema":"announcement","Payload":{"Addrs":[],"ID":"`+pid2.String()+`","Protocols":[],"TTL":60000}}]}`)
	})

	t.Run("PUT /routing/v1/peers (NDJSON Response)", func(t *testing.T) {
		runPutTest(t, mediaTypeNDJSON, `{"Schema":"announcement","Payload":{"Addrs":[],"ID":"`+pid1.String()+`","Protocols":[],"TTL":3600000}}`+"\n"+`{"Schema":"announcement","Payload":{"Addrs":[],"ID":"`+pid2.String()+`","Protocols":[],"TTL":60000}}`+"\n")
	})
}

func makeName(t *testing.T) (crypto.PrivKey, ipns.Name) {
	sk, pid := makePeerID(t)
	return sk, ipns.NameFromPeer(pid)
}

func makeIPNSRecord(t *testing.T, cid cid.Cid, sk crypto.PrivKey, opts ...ipns.Option) (*ipns.Record, []byte) {
	path := path.FromCid(cid)
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

func (m *mockContentRouter) Provide(ctx context.Context, req *ProvideRequest) (time.Duration, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(time.Duration), args.Error(1)
}

func (m *mockContentRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	args := m.Called(ctx, pid, limit)
	return args.Get(0).(iter.ResultIter[*types.PeerRecord]), args.Error(1)
}

func (m *mockContentRouter) ProvidePeer(ctx context.Context, req *ProvidePeerRequest) (time.Duration, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(time.Duration), args.Error(1)
}

func (m *mockContentRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	args := m.Called(ctx, name)
	return args.Get(0).(*ipns.Record), args.Error(1)
}

func (m *mockContentRouter) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	args := m.Called(ctx, name, record)
	return args.Error(0)
}
