package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
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
	"github.com/libp2p/go-libp2p/core/routing"
	b58 "github.com/mr-tron/base58/base58"
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
	require.Equal(t, "Accept", resp.Header.Get("Vary"))

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

func requireCloseToNow(t *testing.T, lastModified string) {
	// inspecting fields like 'Last-Modified'  is prone to one-off errors, we test with 1m buffer
	lastModifiedTime, err := time.Parse(http.TimeFormat, lastModified)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now(), lastModifiedTime, 1*time.Minute)
}

func TestProviders(t *testing.T) {
	// Prepare some variables common to all tests.
	sk1, pid1 := makePeerID(t)
	pid1Str := pid1.String()

	sk2, pid2 := makePeerID(t)
	pid2Str := pid2.String()

	cid1 := makeCID(t)
	cid1Str := cid1.String()

	runGetTest := func(t *testing.T, contentType string, empty bool, expectedStream bool, expectedBody string) {
		t.Parallel()

		var results *iter.SliceIter[iter.Result[types.Record]]

		if empty {
			results = iter.FromSlice([]iter.Result[types.Record]{})
		} else {
			results = iter.FromSlice([]iter.Result[types.Record]{
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
		}

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

		if contentType == "" || strings.Contains(contentType, mediaTypeWildcard) {
			// When no Accept header is provided with request
			// we default expected response to  JSON
			contentType = mediaTypeJSON
		} else {
			req.Header.Set("Accept", contentType)
		}

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		if empty {
			require.Equal(t, 404, resp.StatusCode)
		} else {
			require.Equal(t, 200, resp.StatusCode)
		}

		require.Equal(t, contentType, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))

		if empty {
			require.Equal(t, "public, max-age=15, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))
		} else {
			require.Equal(t, "public, max-age=300, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))
		}
		requireCloseToNow(t, resp.Header.Get("Last-Modified"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		require.Equal(t, expectedBody, string(body))
	}

	t.Run("GET /routing/v1/peers/{cid} (JSON Response)", func(t *testing.T) {
		runGetTest(t, mediaTypeJSON, false, false, `{"Providers":[{"Addrs":[],"ID":"`+pid1Str+`","Protocols":["transport-bitswap"],"Schema":"peer"},{"Addrs":[],"ID":"`+pid2Str+`","Protocols":["transport-bitswap"],"Schema":"peer"}]}`)
	})

	t.Run("GET /routing/v1/peers/{cid} (Empty JSON Response)", func(t *testing.T) {
		runGetTest(t, mediaTypeJSON, true, false, `{"Providers":null}`)
	})

	t.Run("GET /routing/v1/peers/{cid} (Wildcard Accept header defaults to JSON Response)", func(t *testing.T) {
		accept := "text/html,*/*"
		runGetTest(t, accept, true, false, `{"Providers":null}`)
	})

	t.Run("GET /routing/v1/peers/{cid} (Missing Accept header defaults to JSON Response)", func(t *testing.T) {
		accept := ""
		runGetTest(t, accept, true, false, `{"Providers":null}`)
	})

	t.Run("GET /routing/v1/peers/{cid} (NDJSON Response)", func(t *testing.T) {
		runGetTest(t, mediaTypeNDJSON, false, true, `{"Addrs":[],"ID":"`+pid1Str+`","Protocols":["transport-bitswap"],"Schema":"peer"}`+"\n"+`{"Addrs":[],"ID":"`+pid2Str+`","Protocols":["transport-bitswap"],"Schema":"peer"}`+"\n")
	})

	t.Run("GET /routing/v1/peers/{cid} (Empty NDJSON Response)", func(t *testing.T) {
		runGetTest(t, mediaTypeNDJSON, true, true, "")
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

		router.On("Provide", mock.Anything, rec1).Return(time.Hour, nil)

		router.On("Provide", mock.Anything, rec2).Return(time.Minute, nil)

		urlStr := serverAddr + "/routing/v1/providers"

		httpReq, err := http.NewRequest(http.MethodPost, urlStr, bytes.NewReader(body))
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

	t.Run("POST /routing/v1/providers (JSON Response)", func(t *testing.T) {
		runPutTest(t, mediaTypeJSON, `{"ProvideResults":[{"Schema":"announcement-response","TTL":3600000},{"Schema":"announcement-response","TTL":60000}]}`)
	})

	t.Run("POST /routing/v1/providers (NDJSON Response)", func(t *testing.T) {
		runPutTest(t, mediaTypeNDJSON, `{"Schema":"announcement-response","TTL":3600000}`+"\n"+`{"Schema":"announcement-response","TTL":60000}`+"\n")
	})

	t.Run("404 when router returns routing.ErrNotFound", func(t *testing.T) {
		t.Parallel()
		router := &mockContentRouter{}
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)
		serverAddr := "http://" + server.Listener.Addr().String()
		router.On("FindProviders", mock.Anything, cid1, DefaultRecordsLimit).Return(nil, routing.ErrNotFound)

		req, err := http.NewRequest(http.MethodGet, serverAddr+"/routing/v1/providers/"+cid1Str, nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, 404, resp.StatusCode)
	})
}

func TestPeers(t *testing.T) {
	makeGetRequest := func(t *testing.T, router *mockContentRouter, contentType, arg string) *http.Response {
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)
		req, err := http.NewRequest(http.MethodGet, "http://"+server.Listener.Addr().String()+"/routing/v1/peers/"+arg, nil)
		require.NoError(t, err)
		if contentType != "" {
			req.Header.Set("Accept", contentType)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		return resp
	}

	t.Run("GET /routing/v1/peers/{non-peer-valid-cid} returns 400", func(t *testing.T) {
		t.Parallel()

		router := &mockContentRouter{}
		resp := makeGetRequest(t, router, mediaTypeJSON, "bafkqaaa")
		require.Equal(t, 400, resp.StatusCode)
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 404 with correct body and headers (No Results, explicit JSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makePeerID(t)
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		router := &mockContentRouter{}
		router.On("FindPeers", mock.Anything, pid, 20).Return(results, nil)

		resp := makeGetRequest(t, router, mediaTypeJSON, peer.ToCid(pid).String())
		require.Equal(t, 404, resp.StatusCode)

		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, "public, max-age=15, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

		requireCloseToNow(t, resp.Header.Get("Last-Modified"))
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 404 with correct body and headers (No Results, implicit JSON, wildcard Accept header)", func(t *testing.T) {
		t.Parallel()

		_, pid := makePeerID(t)
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		router := &mockContentRouter{}
		router.On("FindPeers", mock.Anything, pid, 20).Return(results, nil)

		// Simulate request with Accept header that includes wildcard match
		resp := makeGetRequest(t, router, "text/html,*/*", peer.ToCid(pid).String())

		// Expect response to default to application/json
		require.Equal(t, 404, resp.StatusCode)
		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))

	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 404 with correct body and headers (No Results, implicit JSON, no Accept header)", func(t *testing.T) {
		t.Parallel()

		_, pid := makePeerID(t)
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		router := &mockContentRouter{}
		router.On("FindPeers", mock.Anything, pid, 20).Return(results, nil)

		// Simulate request without Accept header
		resp := makeGetRequest(t, router, "", peer.ToCid(pid).String())

		// Expect response to default to application/json
		require.Equal(t, 404, resp.StatusCode)
		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 404 when router returns routing.ErrNotFound", func(t *testing.T) {
		t.Parallel()

		_, pid := makePeerID(t)

		router := &mockContentRouter{}
		router.On("FindPeers", mock.Anything, pid, 20).Return(nil, routing.ErrNotFound)

		// Simulate request without Accept header
		resp := makeGetRequest(t, router, "", peer.ToCid(pid).String())

		// Expect response to default to application/json
		require.Equal(t, 404, resp.StatusCode)
		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 200 with correct body and headers (JSON)", func(t *testing.T) {
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

		libp2pKeyCID := peer.ToCid(pid).String()
		resp := makeGetRequest(t, router, mediaTypeJSON, libp2pKeyCID)
		require.Equal(t, 200, resp.StatusCode)

		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, "public, max-age=300, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

		requireCloseToNow(t, resp.Header.Get("Last-Modified"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedBody := `{"Peers":[{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-bitswap","transport-foo"],"Schema":"peer"},{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-foo"],"Schema":"peer"}]}`
		require.Equal(t, expectedBody, string(body))
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 404 with correct body and headers (No Results, NDJSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makePeerID(t)
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		router := &mockContentRouter{}
		router.On("FindPeers", mock.Anything, pid, 0).Return(results, nil)

		resp := makeGetRequest(t, router, mediaTypeNDJSON, peer.ToCid(pid).String())
		require.Equal(t, 404, resp.StatusCode)

		require.Equal(t, mediaTypeNDJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, "public, max-age=15, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

		requireCloseToNow(t, resp.Header.Get("Last-Modified"))
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 200 with correct body and headers (NDJSON)", func(t *testing.T) {
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

		libp2pKeyCID := peer.ToCid(pid).String()
		resp := makeGetRequest(t, router, mediaTypeNDJSON, libp2pKeyCID)
		require.Equal(t, 200, resp.StatusCode)

		require.Equal(t, mediaTypeNDJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, "public, max-age=300, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedBody := `{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-bitswap","transport-foo"],"Schema":"peer"}` + "\n" + `{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-foo"],"Schema":"peer"}` + "\n"
		require.Equal(t, expectedBody, string(body))
	})

	t.Run("GET /routing/v1/peers/{legacy-base58-peer-id} returns 200 with correct body (JSON)", func(t *testing.T) {
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

		legacyPeerID := b58.Encode([]byte(pid))
		resp := makeGetRequest(t, router, mediaTypeJSON, legacyPeerID)
		require.Equal(t, 200, resp.StatusCode)

		header := resp.Header.Get("Content-Type")
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, mediaTypeJSON, header)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedBody := `{"Peers":[{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-bitswap","transport-foo"],"Schema":"peer"},{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-foo"],"Schema":"peer"}]}`
		require.Equal(t, expectedBody, string(body))
	})

	t.Run("GET /routing/v1/peers/{legacy-base58-peer-id} returns 200 with correct body (NDJSON)", func(t *testing.T) {
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

		legacyPeerID := b58.Encode([]byte(pid))
		resp := makeGetRequest(t, router, mediaTypeNDJSON, legacyPeerID)
		require.Equal(t, 200, resp.StatusCode)

		header := resp.Header.Get("Content-Type")
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
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

		req := tjson.AnnouncePeersRequest{Peers: []types.Record{rec1, rec2}}
		body, err := json.Marshal(req)
		require.NoError(t, err)

		router := &mockContentRouter{}
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)

		serverAddr := "http://" + server.Listener.Addr().String()

		router.On("ProvidePeer", mock.Anything, rec1).Return(time.Hour, nil)

		router.On("ProvidePeer", mock.Anything, rec2).Return(time.Minute, nil)

		urlStr := serverAddr + "/routing/v1/peers"

		httpReq, err := http.NewRequest(http.MethodPost, urlStr, bytes.NewReader(body))
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

	t.Run("POST /routing/v1/peers (JSON Response)", func(t *testing.T) {
		runPutTest(t, mediaTypeJSON, `{"ProvideResults":[{"Schema":"announcement-response","TTL":3600000},{"Schema":"announcement-response","TTL":60000}]}`)
	})

	t.Run("POST /routing/v1/peers (NDJSON Response)", func(t *testing.T) {
		runPutTest(t, mediaTypeNDJSON, `{"Schema":"announcement-response","TTL":3600000}`+"\n"+`{"Schema":"announcement-response","TTL":60000}`+"\n")
	})
}

func makeName(t *testing.T) (crypto.PrivKey, ipns.Name) {
	sk, pid := makePeerID(t)
	return sk, ipns.NameFromPeer(pid)
}

func makeIPNSRecord(t *testing.T, cid cid.Cid, eol time.Time, ttl time.Duration, sk crypto.PrivKey, opts ...ipns.Option) (*ipns.Record, []byte) {
	path := path.FromCid(cid)

	record, err := ipns.NewRecord(sk, path, 1, eol, ttl, opts...)
	require.NoError(t, err)

	rawRecord, err := ipns.MarshalRecord(record)
	require.NoError(t, err)

	return record, rawRecord
}

func TestIPNS(t *testing.T) {
	cid1, err := cid.Decode("bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	require.NoError(t, err)

	makeRequest := func(t *testing.T, router *mockContentRouter, path string, accept string) *http.Response {
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)
		serverAddr := "http://" + server.Listener.Addr().String()
		urlStr := serverAddr + path
		req, err := http.NewRequest(http.MethodGet, urlStr, nil)
		require.NoError(t, err)
		if accept != "" {
			req.Header.Set("Accept", accept)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		return resp
	}

	runWithRecordOptions := func(t *testing.T, opts ...ipns.Option) {
		sk, name1 := makeName(t)
		now := time.Now()
		eol := now.Add(24 * time.Hour * 7) // record valid for a week
		ttl := 42 * time.Second            // distinct TTL
		record1, rawRecord1 := makeIPNSRecord(t, cid1, eol, ttl, sk)

		stringToDuration := func(s string) time.Duration {
			seconds, err := strconv.Atoi(s)
			if err != nil {
				return 0
			}
			return time.Duration(seconds) * time.Second
		}

		_, name2 := makeName(t)

		t.Run("GET /routing/v1/ipns/{cid-peer-id} returns 200 (explicit Accept header)", func(t *testing.T) {
			t.Parallel()

			rec, err := ipns.UnmarshalRecord(rawRecord1)
			require.NoError(t, err)

			router := &mockContentRouter{}
			router.On("GetIPNS", mock.Anything, name1).Return(rec, nil)

			resp := makeRequest(t, router, "/routing/v1/ipns/"+name1.String(), mediaTypeIPNSRecord)
			require.Equal(t, 200, resp.StatusCode)
			require.Equal(t, mediaTypeIPNSRecord, resp.Header.Get("Content-Type"))
			require.Equal(t, "Accept", resp.Header.Get("Vary"))
			require.NotEmpty(t, resp.Header.Get("Etag"))

			requireCloseToNow(t, resp.Header.Get("Last-Modified"))

			require.Contains(t, resp.Header.Get("Content-Disposition"), `attachment; filename="`+name1.String()+`.ipns-record"`)

			require.Contains(t, resp.Header.Get("Cache-Control"), "public, max-age=42")

			// expected "stale" values are int(time.Until(eol).Seconds())
			// but running test on slow machine may  be off by a few seconds
			// and we need to assert with some room for drift (1 minute just to not break any CI)
			re := regexp.MustCompile(`(?:^|,\s*)(max-age|stale-while-revalidate|stale-if-error)=(\d+)`)
			matches := re.FindAllStringSubmatch(resp.Header.Get("Cache-Control"), -1)
			staleWhileRevalidate := stringToDuration(matches[1][2])
			staleWhileError := stringToDuration(matches[2][2])
			require.WithinDuration(t, eol, time.Now().Add(staleWhileRevalidate), 1*time.Minute)
			require.WithinDuration(t, eol, time.Now().Add(staleWhileError), 1*time.Minute)

			// 'Expires' on IPNS result is expected to match EOL of IPNS Record with ValidityType=0
			require.Equal(t, eol.UTC().Format(http.TimeFormat), resp.Header.Get("Expires"))

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, body, rawRecord1)
		})

		t.Run("GET /routing/v1/ipns/{cid-peer-id} returns 200 (Accept header missing)", func(t *testing.T) {
			t.Parallel()

			rec, err := ipns.UnmarshalRecord(rawRecord1)
			require.NoError(t, err)

			router := &mockContentRouter{}
			router.On("GetIPNS", mock.Anything, name1).Return(rec, nil)

			// Simulate request without explicit Accept header
			noAccept := ""
			resp := makeRequest(t, router, "/routing/v1/ipns/"+name1.String(), noAccept)

			// Expect application/vnd.ipfs.ipns-record in response
			require.Equal(t, 200, resp.StatusCode)
			require.Equal(t, mediaTypeIPNSRecord, resp.Header.Get("Content-Type"))

			// Confirm body matches  expected bytes
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, body, rawRecord1)
		})

		t.Run("GET /routing/v1/ipns/{cid-peer-id} returns 200 (Accept header with wildcard)", func(t *testing.T) {
			t.Parallel()

			rec, err := ipns.UnmarshalRecord(rawRecord1)
			require.NoError(t, err)

			router := &mockContentRouter{}
			router.On("GetIPNS", mock.Anything, name1).Return(rec, nil)

			// Simulate request with wildcard Accept header
			wcAccept := "text/html,*/*"
			resp := makeRequest(t, router, "/routing/v1/ipns/"+name1.String(), wcAccept)

			// Expect application/vnd.ipfs.ipns-record in response
			require.Equal(t, 200, resp.StatusCode)
			require.Equal(t, mediaTypeIPNSRecord, resp.Header.Get("Content-Type"))

			// Confirm body matches  expected bytes
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, body, rawRecord1)
		})

		t.Run("GET /routing/v1/ipns/{non-peer-cid} returns 400", func(t *testing.T) {
			t.Parallel()

			router := &mockContentRouter{}
			resp := makeRequest(t, router, "/routing/v1/ipns/"+cid1.String(), mediaTypeIPNSRecord)
			require.Equal(t, 400, resp.StatusCode)
		})

		t.Run("GET /routing/v1/ipns/{peer-id} returns 400", func(t *testing.T) {
			t.Parallel()

			router := &mockContentRouter{}
			resp := makeRequest(t, router, "/routing/v1/ipns/"+name1.Peer().String(), mediaTypeIPNSRecord)
			require.Equal(t, 400, resp.StatusCode)
		})

		t.Run("GET /routing/v1/ipns/{cid-peer-id} returns 404 (no record found)", func(t *testing.T) {
			t.Parallel()

			router := &mockContentRouter{}
			router.On("GetIPNS", mock.Anything, name1).Return(nil, routing.ErrNotFound)

			// Simulate request without explicit Accept header
			noAccept := ""
			resp := makeRequest(t, router, "/routing/v1/ipns/"+name1.String(), noAccept)

			// Expect application/vnd.ipfs.ipns-record in response
			require.Equal(t, 404, resp.StatusCode)
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
	a := args.Get(0)
	if a == nil {
		return nil, args.Error(1)
	}
	return a.(iter.ResultIter[types.Record]), args.Error(1)
}

func (m *mockContentRouter) Provide(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(time.Duration), args.Error(1)
}

func (m *mockContentRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	args := m.Called(ctx, pid, limit)
	a := args.Get(0)
	if a == nil {
		return nil, args.Error(1)
	}
	return a.(iter.ResultIter[*types.PeerRecord]), args.Error(1)
}

func (m *mockContentRouter) ProvidePeer(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(time.Duration), args.Error(1)
}

func (m *mockContentRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	args := m.Called(ctx, name)
	a := args.Get(0)
	if a == nil {
		return nil, args.Error(1)
	}
	return a.(*ipns.Record), args.Error(1)
}

func (m *mockContentRouter) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	args := m.Called(ctx, name, record)
	return args.Error(0)
}
