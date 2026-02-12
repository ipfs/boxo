package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
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
	"github.com/ipfs/boxo/routing/http/filters"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	b58 "github.com/mr-tron/base58/base58"
	"github.com/multiformats/go-multiaddr"
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
		}},
	})

	c := "baeabep4vu3ceru7nerjjbk37sxb7wmftteve4hcosmyolsbsiubw2vr6pqzj6mw7kv6tbn6nqkkldnklbjgm5tzbi4hkpkled4xlcr7xz4bq"
	cb, err := cid.Decode(c)
	require.NoError(t, err)

	router.On("FindProviders", mock.Anything, cb, DefaultRecordsLimit).
		Return(results, nil)

	resp, err := http.Get(serverAddr + "/routing/v1/providers/" + c)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
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

func makeEd25519PeerID(t *testing.T) (crypto.PrivKey, peer.ID) {
	sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	pid, err := peer.IDFromPrivateKey(sk)
	require.NoError(t, err)

	return sk, pid
}

func makeLegacyRSAPeerID(t *testing.T) (crypto.PrivKey, peer.ID) {
	sk, _, err := crypto.GenerateRSAKeyPair(2048, rand.Reader)
	require.NoError(t, err)

	pid, err := peer.IDFromPrivateKey(sk)
	require.NoError(t, err)

	return sk, pid
}

func requireCloseToNow(t *testing.T, lastModified string) {
	// inspecting fields like 'Last-Modified'  is prone to one-off errors, we test with 1m buffer
	lastModifiedTime, err := time.Parse(http.TimeFormat, lastModified)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now(), lastModifiedTime, 1*time.Minute)
}

func TestProviders(t *testing.T) {
	pidStr := "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn"
	pid2Str := "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vz"
	cidStr := "bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4"

	addr1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
	addr2, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/udp/4001/quic-v1")
	addr3, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001/ws")
	addr4, _ := multiaddr.NewMultiaddr("/ip4/102.101.1.1/tcp/4001/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit")
	addr5, _ := multiaddr.NewMultiaddr("/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit")
	addr6, _ := multiaddr.NewMultiaddr("/ip4/8.8.8.8/udp/4001/quic-v1/webtransport")

	pid, err := peer.Decode(pidStr)
	require.NoError(t, err)
	pid2, err := peer.Decode(pid2Str)
	require.NoError(t, err)

	cid, err := cid.Decode(cidStr)
	require.NoError(t, err)

	runTest := func(t *testing.T, contentType string, filterAddrs, filterProtocols string, empty bool, expectedStream bool, expectedBody string) {
		t.Parallel()

		var results *iter.SliceIter[iter.Result[types.Record]]

		if empty {
			results = iter.FromSlice([]iter.Result[types.Record]{})
		} else {
			results = iter.FromSlice([]iter.Result[types.Record]{
				{Val: &types.PeerRecord{
					Schema:    types.SchemaPeer,
					ID:        &pid,
					Protocols: []string{"transport-bitswap"},
					Addrs: []types.Multiaddr{
						{Multiaddr: addr1},
						{Multiaddr: addr2},
						{Multiaddr: addr3},
						{Multiaddr: addr4},
						{Multiaddr: addr5},
						{Multiaddr: addr6},
					},
				}},
				{Val: &types.PeerRecord{
					Schema:    types.SchemaPeer,
					ID:        &pid2,
					Protocols: []string{"transport-ipfs-gateway-http"},
					Addrs:     []types.Multiaddr{},
				}},
			},
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
		router.On("FindProviders", mock.Anything, cid, limit).Return(results, nil)

		urlStr := fmt.Sprintf("%s/routing/v1/providers/%s", serverAddr, cidStr)
		urlStr = filters.AddFiltersToURL(urlStr, strings.Split(filterProtocols, ","), strings.Split(filterAddrs, ","))

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
		// Per IPIP-0513: Always return 200, even for empty results
		require.Equal(t, http.StatusOK, resp.StatusCode)

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

	t.Run("JSON Response", func(t *testing.T) {
		runTest(t, mediaTypeJSON, "", "", false, false, `{"Providers":[{"Addrs":["/ip4/127.0.0.1/tcp/4001","/ip4/127.0.0.1/udp/4001/quic-v1","/ip4/127.0.0.1/tcp/4001/ws","/ip4/102.101.1.1/tcp/4001/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit","/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit","/ip4/8.8.8.8/udp/4001/quic-v1/webtransport"],"ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn","Protocols":["transport-bitswap"],"Schema":"peer"},{"Addrs":[],"ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vz","Protocols":["transport-ipfs-gateway-http"],"Schema":"peer"}]}`)
	})

	t.Run("JSON Response with addr filtering including unknown", func(t *testing.T) {
		runTest(t, mediaTypeJSON, "webtransport,!p2p-circuit,unknown", "", false, false, `{"Providers":[{"Addrs":["/ip4/8.8.8.8/udp/4001/quic-v1/webtransport"],"ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn","Protocols":["transport-bitswap"],"Schema":"peer"},{"Addrs":[],"ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vz","Protocols":["transport-ipfs-gateway-http"],"Schema":"peer"}]}`)
	})

	t.Run("JSON Response with addr filtering", func(t *testing.T) {
		runTest(t, mediaTypeJSON, "webtransport,!p2p-circuit", "", false, false, `{"Providers":[{"Addrs":["/ip4/8.8.8.8/udp/4001/quic-v1/webtransport"],"ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn","Protocols":["transport-bitswap"],"Schema":"peer"}]}`)
	})

	t.Run("JSON Response with protocol and addr filtering", func(t *testing.T) {
		runTest(t, mediaTypeJSON, "quic-v1", "transport-bitswap", false, false,
			`{"Providers":[{"Addrs":["/ip4/127.0.0.1/udp/4001/quic-v1","/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit","/ip4/8.8.8.8/udp/4001/quic-v1/webtransport"],"ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn","Protocols":["transport-bitswap"],"Schema":"peer"}]}`)
	})

	t.Run("JSON Response with protocol filtering", func(t *testing.T) {
		runTest(t, mediaTypeJSON, "", "transport-ipfs-gateway-http", false, false,
			`{"Providers":[{"Addrs":[],"ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vz","Protocols":["transport-ipfs-gateway-http"],"Schema":"peer"}]}`)
	})

	t.Run("Empty JSON Response", func(t *testing.T) {
		runTest(t, mediaTypeJSON, "", "", true, false, `{"Providers":[]}`)
	})

	t.Run("Wildcard Accept header defaults to JSON Response", func(t *testing.T) {
		accept := "text/html,*/*"
		runTest(t, accept, "", "", true, false, `{"Providers":[]}`)
	})

	t.Run("Missing Accept header defaults to JSON Response", func(t *testing.T) {
		accept := ""
		runTest(t, accept, "", "", true, false, `{"Providers":[]}`)
	})

	t.Run("NDJSON Response", func(t *testing.T) {
		runTest(t, mediaTypeNDJSON, "", "", false, true, `{"Addrs":["/ip4/127.0.0.1/tcp/4001","/ip4/127.0.0.1/udp/4001/quic-v1","/ip4/127.0.0.1/tcp/4001/ws","/ip4/102.101.1.1/tcp/4001/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit","/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit","/ip4/8.8.8.8/udp/4001/quic-v1/webtransport"],"ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn","Protocols":["transport-bitswap"],"Schema":"peer"}`+"\n"+`{"Addrs":[],"ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vz","Protocols":["transport-ipfs-gateway-http"],"Schema":"peer"}`+"\n")
	})

	t.Run("NDJSON Response with addr filtering", func(t *testing.T) {
		runTest(t, mediaTypeNDJSON, "webtransport,!p2p-circuit,unknown", "", false, true, `{"Addrs":["/ip4/8.8.8.8/udp/4001/quic-v1/webtransport"],"ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn","Protocols":["transport-bitswap"],"Schema":"peer"}`+"\n"+`{"Addrs":[],"ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vz","Protocols":["transport-ipfs-gateway-http"],"Schema":"peer"}`+"\n")
	})

	t.Run("Empty NDJSON Response", func(t *testing.T) {
		runTest(t, mediaTypeNDJSON, "", "", true, true, "")
	})

	t.Run("404 when router returns routing.ErrNotFound", func(t *testing.T) {
		t.Parallel()
		router := &mockContentRouter{}
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)
		serverAddr := "http://" + server.Listener.Addr().String()
		router.On("FindProviders", mock.Anything, cid, DefaultRecordsLimit).Return(nil, routing.ErrNotFound)

		req, err := http.NewRequest(http.MethodGet, serverAddr+"/routing/v1/providers/"+cidStr, nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		// Per IPIP-0513: Return 200 for empty results even when ErrNotFound
		require.Equal(t, http.StatusOK, resp.StatusCode)

		// Verify empty response body
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, `{"Providers":[]}`, string(body))
	})
}

// TestProvidersMixedSchemas verifies that the server correctly serializes a mix
// of PeerRecord and GenericRecord in FindProviders responses. This is the
// primary IPIP-518 server-side scenario: a routing backend returns both
// traditional peer records (multiaddrs only) and generic records (with URLs
// and non-standard URI schemes) in the same response stream.
func TestProvidersMixedSchemas(t *testing.T) {
	cidStr := "bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4"
	cb, err := cid.Decode(cidStr)
	require.NoError(t, err)

	pid, err := peer.Decode("12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn")
	require.NoError(t, err)

	addr1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")

	results := iter.FromSlice([]iter.Result[types.Record]{
		// Traditional PeerRecord with multiaddrs
		{Val: &types.PeerRecord{
			Schema:    types.SchemaPeer,
			ID:        &pid,
			Protocols: []string{"transport-bitswap"},
			Addrs:     []types.Multiaddr{{Multiaddr: addr1}},
		}},
		// GenericRecord with URLs (IPIP-518)
		{Val: &types.GenericRecord{
			Schema:    types.SchemaGeneric,
			ID:        "gateway-provider-1",
			Protocols: []string{"transport-ipfs-gateway-http"},
			Addrs: types.Addresses{
				mustAddr(t, "https://gateway.example.com"),
				mustAddr(t, "foo://custom.example.com"),
			},
		}},
	})

	router := &mockContentRouter{}
	server := httptest.NewServer(Handler(router))
	t.Cleanup(server.Close)
	serverAddr := "http://" + server.Listener.Addr().String()

	router.On("FindProviders", mock.Anything, cb, DefaultRecordsLimit).Return(results, nil)

	resp, err := http.Get(serverAddr + "/routing/v1/providers/" + cidStr)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Verify both records are present in the JSON response.
	// PeerRecord uses typed Addrs (multiaddrs), GenericRecord uses string Addrs (URLs).
	bodyStr := string(body)
	require.Contains(t, bodyStr, `"Schema":"peer"`)
	require.Contains(t, bodyStr, `"Schema":"generic"`)
	require.Contains(t, bodyStr, `"/ip4/127.0.0.1/tcp/4001"`)
	require.Contains(t, bodyStr, `"https://gateway.example.com"`)
	require.Contains(t, bodyStr, `"foo://custom.example.com"`)
	require.Contains(t, bodyStr, `"transport-ipfs-gateway-http"`)
}

// TestProvidersMixedSchemasWithFiltering verifies that filter-addrs and
// filter-protocols work correctly when the response contains both PeerRecord
// and GenericRecord. The server must apply filters to all schema types.
func TestProvidersMixedSchemasWithFiltering(t *testing.T) {
	cidStr := "bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4"
	cb, err := cid.Decode(cidStr)
	require.NoError(t, err)

	pid, err := peer.Decode("12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn")
	require.NoError(t, err)

	addr1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")

	results := iter.FromSlice([]iter.Result[types.Record]{
		// PeerRecord with bitswap (no /http in addrs)
		{Val: &types.PeerRecord{
			Schema:    types.SchemaPeer,
			ID:        &pid,
			Protocols: []string{"transport-bitswap"},
			Addrs:     []types.Multiaddr{{Multiaddr: addr1}},
		}},
		// GenericRecord with gateway URL
		{Val: &types.GenericRecord{
			Schema:    types.SchemaGeneric,
			ID:        "gateway-provider-1",
			Protocols: []string{"transport-ipfs-gateway-http"},
			Addrs: types.Addresses{
				mustAddr(t, "https://gateway.example.com"),
			},
		}},
	})

	router := &mockContentRouter{}
	server := httptest.NewServer(Handler(router))
	t.Cleanup(server.Close)
	serverAddr := "http://" + server.Listener.Addr().String()

	router.On("FindProviders", mock.Anything, cb, DefaultRecordsLimit).Return(results, nil)

	// Filter for transport-ipfs-gateway-http protocol only
	urlStr := fmt.Sprintf("%s/routing/v1/providers/%s", serverAddr, cidStr)
	urlStr = filters.AddFiltersToURL(urlStr, []string{"transport-ipfs-gateway-http"}, nil)

	resp, err := http.Get(urlStr)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	bodyStr := string(body)
	// PeerRecord with bitswap should be filtered out
	require.NotContains(t, bodyStr, `"transport-bitswap"`)
	// GenericRecord with gateway should remain
	require.Contains(t, bodyStr, `"Schema":"generic"`)
	require.Contains(t, bodyStr, `"https://gateway.example.com"`)
}

// TestProvidersMixedSchemasNDJSON verifies that the server correctly serializes
// a mix of PeerRecord and GenericRecord in NDJSON streaming responses.
// This complements TestProvidersMixedSchemas which only tests the JSON path.
func TestProvidersMixedSchemasNDJSON(t *testing.T) {
	cidStr := "bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4"
	cb, err := cid.Decode(cidStr)
	require.NoError(t, err)

	pid, err := peer.Decode("12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn")
	require.NoError(t, err)

	addr1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")

	results := iter.FromSlice([]iter.Result[types.Record]{
		{Val: &types.PeerRecord{
			Schema:    types.SchemaPeer,
			ID:        &pid,
			Protocols: []string{"transport-bitswap"},
			Addrs:     []types.Multiaddr{{Multiaddr: addr1}},
		}},
		{Val: &types.GenericRecord{
			Schema:    types.SchemaGeneric,
			ID:        "did:key:z6Mkm1example",
			Protocols: []string{"transport-ipfs-gateway-http"},
			Addrs: types.Addresses{
				mustAddr(t, "https://gateway.example.com"),
				mustAddr(t, "foo://custom.example.com"),
			},
		}},
	})

	router := &mockContentRouter{}
	server := httptest.NewServer(Handler(router))
	t.Cleanup(server.Close)
	serverAddr := "http://" + server.Listener.Addr().String()

	router.On("FindProviders", mock.Anything, cb, DefaultStreamingRecordsLimit).Return(results, nil)

	req, err := http.NewRequest(http.MethodGet, serverAddr+"/routing/v1/providers/"+cidStr, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", mediaTypeNDJSON)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, mediaTypeNDJSON, resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Each NDJSON line is a separate record.
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	require.Len(t, lines, 2, "expected two NDJSON lines (peer + generic)")

	// Line 0: PeerRecord
	require.Contains(t, lines[0], `"Schema":"peer"`)
	require.Contains(t, lines[0], `"/ip4/127.0.0.1/tcp/4001"`)

	// Line 1: GenericRecord with URLs
	require.Contains(t, lines[1], `"Schema":"generic"`)
	require.Contains(t, lines[1], `"did:key:z6Mkm1example"`)
	require.Contains(t, lines[1], `"https://gateway.example.com"`)
	require.Contains(t, lines[1], `"foo://custom.example.com"`)
	require.Contains(t, lines[1], `"transport-ipfs-gateway-http"`)
}

// TestProvidersMixedSchemasExtraFields verifies that protocol-specific metadata
// (Extra fields) on GenericRecord survive server serialization in both JSON
// and NDJSON paths. The spec allows extra fields like transport-ipfs-gateway-http
// to carry protocol-specific metadata (e.g. "foobar" capability bitfield).
func TestProvidersMixedSchemasExtraFields(t *testing.T) {
	cidStr := "bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4"
	cb, err := cid.Decode(cidStr)
	require.NoError(t, err)

	gr := &types.GenericRecord{
		Schema:    types.SchemaGeneric,
		ID:        "QmUA9D3H7HeCYsirB3KmPSvZh3dNXMZas6Lwgr4fv1HTTp",
		Protocols: []string{"transport-ipfs-gateway-http"},
		Addrs:     types.Addresses{mustAddr(t, "https://dag.w3s.link")},
		Extra: map[string]json.RawMessage{
			"transport-ipfs-gateway-http": json.RawMessage(`"foobar"`),
		},
	}

	t.Run("JSON", func(t *testing.T) {
		results := iter.FromSlice([]iter.Result[types.Record]{
			{Val: gr},
		})

		router := &mockContentRouter{}
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)
		serverAddr := "http://" + server.Listener.Addr().String()

		router.On("FindProviders", mock.Anything, cb, DefaultRecordsLimit).Return(results, nil)

		resp, err := http.Get(serverAddr + "/routing/v1/providers/" + cidStr)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		bodyStr := string(body)
		require.Contains(t, bodyStr, `"transport-ipfs-gateway-http"`)
		// Extra field must survive serialization
		require.Contains(t, bodyStr, `"foobar"`)
	})

	t.Run("NDJSON", func(t *testing.T) {
		results := iter.FromSlice([]iter.Result[types.Record]{
			{Val: gr},
		})

		router := &mockContentRouter{}
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)
		serverAddr := "http://" + server.Listener.Addr().String()

		router.On("FindProviders", mock.Anything, cb, DefaultStreamingRecordsLimit).Return(results, nil)

		req, err := http.NewRequest(http.MethodGet, serverAddr+"/routing/v1/providers/"+cidStr, nil)
		require.NoError(t, err)
		req.Header.Set("Accept", mediaTypeNDJSON)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		bodyStr := string(body)
		require.Contains(t, bodyStr, `"transport-ipfs-gateway-http"`)
		require.Contains(t, bodyStr, `"foobar"`)
	})
}

func mustAddr(t *testing.T, s string) types.Address {
	t.Helper()
	addr, err := types.NewAddress(s)
	require.NoError(t, err)
	return addr
}

func TestPeers(t *testing.T) {
	makeRequest := func(t *testing.T, router *mockContentRouter, contentType, arg, filterAddrs, filterProtocols string) *http.Response {
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)

		urlStr := fmt.Sprintf("http://%s/routing/v1/peers/%s", server.Listener.Addr().String(), arg)
		urlStr = filters.AddFiltersToURL(urlStr, strings.Split(filterProtocols, ","), strings.Split(filterAddrs, ","))

		req, err := http.NewRequest(http.MethodGet, urlStr, nil)
		require.NoError(t, err)
		if contentType != "" {
			req.Header.Set("Accept", contentType)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		return resp
	}

	t.Run("GET /routing/v1/peers/{non-peerid} returns 400", func(t *testing.T) {
		t.Parallel()

		router := &mockContentRouter{}
		resp := makeRequest(t, router, mediaTypeJSON, "nonpeerid", "", "")
		require.Equal(t, 400, resp.StatusCode)
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 200 with empty results (No Results, explicit JSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		router := &mockContentRouter{}
		router.On("FindPeers", mock.Anything, pid, DefaultRecordsLimit).Return(results, nil)

		resp := makeRequest(t, router, mediaTypeJSON, peer.ToCid(pid).String(), "", "")
		// Per IPIP-0513: Return 200 for empty results
		require.Equal(t, http.StatusOK, resp.StatusCode)

		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, "public, max-age=15, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

		requireCloseToNow(t, resp.Header.Get("Last-Modified"))

		// Verify the response body contains empty Peers array
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, `{"Peers":[]}`, string(body))
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 200 with empty results (No Results, implicit JSON, wildcard Accept header)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		router := &mockContentRouter{}
		router.On("FindPeers", mock.Anything, pid, DefaultRecordsLimit).Return(results, nil)

		// Simulate request with Accept header that includes wildcard match
		resp := makeRequest(t, router, "text/html,*/*", peer.ToCid(pid).String(), "", "")

		// Per IPIP-0513: Return 200 for empty results
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))

		// Verify empty results
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, `{"Peers":[]}`, string(body))
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 200 with empty results (No Results, implicit JSON, no Accept header)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		router := &mockContentRouter{}
		router.On("FindPeers", mock.Anything, pid, DefaultRecordsLimit).Return(results, nil)

		// Simulate request without Accept header
		resp := makeRequest(t, router, "", peer.ToCid(pid).String(), "", "")

		// Per IPIP-0513: Return 200 for empty results
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))

		// Verify empty results
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, `{"Peers":[]}`, string(body))
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 200 with empty results when router returns routing.ErrNotFound", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)

		router := &mockContentRouter{}
		router.On("FindPeers", mock.Anything, pid, DefaultRecordsLimit).Return(nil, routing.ErrNotFound)

		// Simulate request without Accept header
		resp := makeRequest(t, router, "", peer.ToCid(pid).String(), "", "")

		// Per IPIP-0513: Return 200 for empty results even when ErrNotFound
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))

		// Verify empty results
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, `{"Peers":[]}`, string(body))
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 200 with correct body and headers (JSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
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
		router.On("FindPeers", mock.Anything, pid, DefaultRecordsLimit).Return(results, nil)

		libp2pKeyCID := peer.ToCid(pid).String()
		resp := makeRequest(t, router, mediaTypeJSON, libp2pKeyCID, "", "")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, "public, max-age=300, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

		requireCloseToNow(t, resp.Header.Get("Last-Modified"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedBody := `{"Peers":[{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-bitswap","transport-foo"],"Schema":"peer"},{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-foo"],"Schema":"peer"}]}`
		require.Equal(t, expectedBody, string(body))
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 200 with correct body and headers (JSON) with filter-addrs", func(t *testing.T) {
		t.Parallel()

		addr1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
		addr2, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/udp/4001/quic-v1")
		addr3, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001/ws")
		addr4, _ := multiaddr.NewMultiaddr("/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit")
		addr5, _ := multiaddr.NewMultiaddr("/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu")
		_, pid := makeEd25519PeerID(t)
		_, pid2 := makeEd25519PeerID(t)
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{
			{Val: &types.PeerRecord{
				Schema:    types.SchemaPeer,
				ID:        &pid,
				Protocols: []string{"transport-bitswap", "transport-foo"},
				Addrs: []types.Multiaddr{
					{Multiaddr: addr1},
					{Multiaddr: addr2},
					{Multiaddr: addr3},
					{Multiaddr: addr4},
				},
			}},
			{Val: &types.PeerRecord{
				Schema:    types.SchemaPeer,
				ID:        &pid2,
				Protocols: []string{"transport-foo"},
				Addrs: []types.Multiaddr{
					{Multiaddr: addr5},
				},
			}},
		})

		router := &mockContentRouter{}
		router.On("FindPeers", mock.Anything, pid, DefaultRecordsLimit).Return(results, nil)

		libp2pKeyCID := peer.ToCid(pid).String()
		resp := makeRequest(t, router, mediaTypeJSON, libp2pKeyCID, "tcp", "")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, "public, max-age=300, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

		requireCloseToNow(t, resp.Header.Get("Last-Modified"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedBody := `{"Peers":[{"Addrs":["/ip4/127.0.0.1/tcp/4001","/ip4/127.0.0.1/tcp/4001/ws"],"ID":"` + pid.String() + `","Protocols":["transport-bitswap","transport-foo"],"Schema":"peer"}]}`
		require.Equal(t, expectedBody, string(body))
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 200 with correct body and headers (JSON) with filter-protocols", func(t *testing.T) {
		t.Parallel()

		addr1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
		addr2, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/udp/4001/quic-v1")
		addr3, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001/ws")
		addr4, _ := multiaddr.NewMultiaddr("/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit")
		addr5, _ := multiaddr.NewMultiaddr("/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu")
		_, pid := makeEd25519PeerID(t)
		_, pid2 := makeEd25519PeerID(t)
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{
			{Val: &types.PeerRecord{
				Schema:    types.SchemaPeer,
				ID:        &pid,
				Protocols: []string{"transport-bitswap", "transport-foo"},
				Addrs: []types.Multiaddr{
					{Multiaddr: addr1},
					{Multiaddr: addr2},
					{Multiaddr: addr3},
					{Multiaddr: addr4},
				},
			}},
			{Val: &types.PeerRecord{
				Schema:    types.SchemaPeer,
				ID:        &pid2,
				Protocols: []string{"transport-foo"},
				Addrs: []types.Multiaddr{
					{Multiaddr: addr5},
				},
			}},
		})

		router := &mockContentRouter{}
		router.On("FindPeers", mock.Anything, pid, DefaultRecordsLimit).Return(results, nil)

		libp2pKeyCID := peer.ToCid(pid).String()
		resp := makeRequest(t, router, mediaTypeJSON, libp2pKeyCID, "", "transport-bitswap")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, "public, max-age=300, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

		requireCloseToNow(t, resp.Header.Get("Last-Modified"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedBody := `{"Peers":[{"Addrs":["/ip4/127.0.0.1/tcp/4001","/ip4/127.0.0.1/udp/4001/quic-v1","/ip4/127.0.0.1/tcp/4001/ws","/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit"],"ID":"` + pid.String() + `","Protocols":["transport-bitswap","transport-foo"],"Schema":"peer"}]}`
		require.Equal(t, expectedBody, string(body))
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 200 with empty results (No Results, NDJSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		router := &mockContentRouter{}
		router.On("FindPeers", mock.Anything, pid, DefaultStreamingRecordsLimit).Return(results, nil)

		resp := makeRequest(t, router, mediaTypeNDJSON, peer.ToCid(pid).String(), "", "")
		// Per IPIP-0513: Return 200 for empty results
		require.Equal(t, http.StatusOK, resp.StatusCode)

		require.Equal(t, mediaTypeNDJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, "public, max-age=15, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

		requireCloseToNow(t, resp.Header.Get("Last-Modified"))

		// Verify empty NDJSON stream (no lines)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, "", string(body))
	})

	t.Run("GET /routing/v1/peers/{cid-libp2p-key-peer-id} returns 200 with correct body and headers (NDJSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
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
		router.On("FindPeers", mock.Anything, pid, DefaultStreamingRecordsLimit).Return(results, nil)

		libp2pKeyCID := peer.ToCid(pid).String()
		resp := makeRequest(t, router, mediaTypeNDJSON, libp2pKeyCID, "", "")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		require.Equal(t, mediaTypeNDJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, "public, max-age=300, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedBody := `{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-bitswap","transport-foo"],"Schema":"peer"}` + "\n" + `{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-foo"],"Schema":"peer"}` + "\n"
		require.Equal(t, expectedBody, string(body))
	})

	// Test matrix that runs the HTTP 200 scenario against different flavours of PeerID
	// to ensure consistent behavior
	peerIdtestCases := []struct {
		peerIdType    string
		makePeerId    func(t *testing.T) (crypto.PrivKey, peer.ID)
		peerIdAsCidV1 bool
	}{
		// Test against current and past PeerID key types and string representations.
		// https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation
		{"cidv1-libp2p-key-ed25519-peerid", makeEd25519PeerID, true},
		{"base58-ed25519-peerid", makeEd25519PeerID, false},
		{"cidv1-libp2p-key-rsa-peerid", makeLegacyRSAPeerID, true},
		{"base58-rsa-peerid", makeLegacyRSAPeerID, false},
	}

	for _, tc := range peerIdtestCases {
		_, pid := tc.makePeerId(t)
		var peerIDStr string
		if tc.peerIdAsCidV1 {
			// PeerID represented by CIDv1 with libp2p-key codec
			// https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation
			peerIDStr = peer.ToCid(pid).String()
		} else {
			// Legacy PeerID starting with "123..." or "Qm.."
			// https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation
			peerIDStr = b58.Encode([]byte(pid))
		}
		results := []iter.Result[*types.PeerRecord]{
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
		}

		t.Run("GET /routing/v1/peers/{"+tc.peerIdType+"} returns 200 with correct body and headers (NDJSON streaming response)", func(t *testing.T) {
			t.Parallel()

			router := &mockContentRouter{}
			router.On("FindPeers", mock.Anything, pid, DefaultStreamingRecordsLimit).Return(iter.FromSlice(results), nil)

			resp := makeRequest(t, router, mediaTypeNDJSON, peerIDStr, "", "")
			require.Equal(t, http.StatusOK, resp.StatusCode)

			require.Equal(t, mediaTypeNDJSON, resp.Header.Get("Content-Type"))
			require.Equal(t, "Accept", resp.Header.Get("Vary"))
			require.Equal(t, "public, max-age=300, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			expectedBody := `{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-bitswap","transport-foo"],"Schema":"peer"}` + "\n" + `{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-foo"],"Schema":"peer"}` + "\n"
			require.Equal(t, expectedBody, string(body))
		})

		t.Run("GET /routing/v1/peers/{"+tc.peerIdType+"} returns 200 with correct body and headers (JSON response)", func(t *testing.T) {
			t.Parallel()

			router := &mockContentRouter{}
			router.On("FindPeers", mock.Anything, pid, DefaultRecordsLimit).Return(iter.FromSlice(results), nil)

			resp := makeRequest(t, router, mediaTypeJSON, peerIDStr, "", "")
			require.Equal(t, http.StatusOK, resp.StatusCode)

			require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
			require.Equal(t, "Accept", resp.Header.Get("Vary"))
			require.Equal(t, "public, max-age=300, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			expectedBody := `{"Peers":[{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-bitswap","transport-foo"],"Schema":"peer"},{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-foo"],"Schema":"peer"}]}`
			require.Equal(t, expectedBody, string(body))
		})
	}
}

func TestParseKey(t *testing.T) {
	t.Run("parses arbitrary CID", func(t *testing.T) {
		cidStr := "bafkreidcd7frenco2m6ch7mny63wztgztv3q6fctaffgowkro6kljre5ei"
		expectedCID, err := cid.Decode(cidStr)
		require.NoError(t, err)

		parsedCID, err := parseKey(cidStr)
		require.NoError(t, err)
		require.Equal(t, expectedCID, parsedCID)
	})

	t.Run("parses Ed25519 PeerID as CIDv1 libp2p-key", func(t *testing.T) {
		// Example from libp2p specs
		// https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation
		cidStr := "bafzbeie5745rpv2m6tjyuugywy4d5ewrqgqqhfnf445he3omzpjbx5xqxe"
		pid, err := peer.Decode(cidStr)
		require.NoError(t, err)
		expectedCID := peer.ToCid(pid)

		parsedCID, err := parseKey(cidStr)
		require.NoError(t, err)
		require.Equal(t, expectedCID, parsedCID)
	})

	t.Run("parses Ed25519 PeerID as Base58", func(t *testing.T) {
		// Example from libp2p specs (identity multihash)
		// https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation
		pidStr := "12D3KooWD3eckifWpRn9wQpMG9R9hX3sD158z7EqHWmweQAJU5SA"
		pid, err := peer.Decode(pidStr)
		require.NoError(t, err)
		expectedCID := peer.ToCid(pid)

		parsedCID, err := parseKey(pidStr)
		require.NoError(t, err)
		require.Equal(t, expectedCID, parsedCID)
	})

	t.Run("parses RSA PeerID as CIDv1 libp2p-key", func(t *testing.T) {
		// RSA PeerID starting with "Qm" encoded as CIDv1
		// https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation
		pidStr := "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N"
		pid, err := peer.Decode(pidStr)
		require.NoError(t, err)
		// Convert to CIDv1 representation
		cidStr := peer.ToCid(pid).String()
		expectedCID := peer.ToCid(pid)

		parsedCID, err := parseKey(cidStr)
		require.NoError(t, err)
		require.Equal(t, expectedCID, parsedCID)
	})

	t.Run("parses RSA PeerID as Base58", func(t *testing.T) {
		// Example from libp2p specs (SHA256-based)
		// https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation
		pidStr := "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N"
		pid, err := peer.Decode(pidStr)
		require.NoError(t, err)
		expectedCID := peer.ToCid(pid)

		parsedCID, err := parseKey(pidStr)
		require.NoError(t, err)
		require.Equal(t, expectedCID, parsedCID)
	})

	t.Run("returns error for invalid string", func(t *testing.T) {
		_, err := parseKey("not-a-valid-cid-or-peerid")
		require.Error(t, err)
		require.Contains(t, err.Error(), "unable to parse as CID or PeerID")
	})
}

func TestGetClosestPeers(t *testing.T) {
	makeRequest := func(t *testing.T, router *mockContentRouter, contentType, arg string) *http.Response {
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)

		urlStr := fmt.Sprintf("http://%s/routing/v1/dht/closest/peers/%s", server.Listener.Addr().String(), arg)
		t.Log(urlStr)

		req, err := http.NewRequest(http.MethodGet, urlStr, nil)
		require.NoError(t, err)
		if contentType != "" {
			req.Header.Set("Accept", contentType)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		return resp
	}

	t.Run("GET /routing/v1/dht/closest/peers/{non-cid} returns 400", func(t *testing.T) {
		t.Parallel()

		router := &mockContentRouter{}
		resp := makeRequest(t, router, mediaTypeJSON, "nonpeerid")
		require.Equal(t, 400, resp.StatusCode)
	})

	t.Run("GET /routing/v1/dht/closest/peers/{cid} returns 200 with correct body and headers (No Results, explicit JSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		router := &mockContentRouter{}
		router.On("GetClosestPeers", mock.Anything, key).Return(results, nil)

		resp := makeRequest(t, router, mediaTypeJSON, key.String())
		require.Equal(t, 200, resp.StatusCode)

		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, "public, max-age=15, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

		requireCloseToNow(t, resp.Header.Get("Last-Modified"))
	})

	t.Run("GET /routing/v1/dht/closest/peers/{cid} returns 200 with correct body and headers (No Results, implicit JSON, wildcard Accept header)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		router := &mockContentRouter{}
		router.On("GetClosestPeers", mock.Anything, key).Return(results, nil)

		// Simulate request with Accept header that includes wildcard match
		resp := makeRequest(t, router, "text/html,*/*", key.String())

		// Expect response to default to application/json
		require.Equal(t, 200, resp.StatusCode)
		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
	})

	t.Run("GET /routing/v1/dht/closest/peers/{cid} returns 200 with correct body and headers (No Results, implicit JSON, no Accept header)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)
		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		router := &mockContentRouter{}
		router.On("GetClosestPeers", mock.Anything, key).Return(results, nil)

		// Simulate request without Accept header
		resp := makeRequest(t, router, "", key.String())

		// Expect response to default to application/json
		require.Equal(t, 200, resp.StatusCode)
		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
	})

	t.Run("GET /routing/v1/dht/closest/peers/{cid} returns 200 when router returns routing.ErrNotFound", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)

		router := &mockContentRouter{}
		router.On("GetClosestPeers", mock.Anything, key).Return(nil, routing.ErrNotFound)

		// Simulate request without Accept header
		resp := makeRequest(t, router, "", key.String())

		// Expect response to default to application/json
		require.Equal(t, 200, resp.StatusCode)
		require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
	})

	t.Run("GET /routing/v1/dht/closest/peers/{cid} returns 200 with correct body and headers (JSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)
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
		router.On("GetClosestPeers", mock.Anything, key).Return(results, nil)

		resp := makeRequest(t, router, mediaTypeJSON, key.String())
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

	t.Run("GET /routing/v1/dht/closest/peers/{cid} returns 200 with correct body and headers (No Results, NDJSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)

		results := iter.FromSlice([]iter.Result[*types.PeerRecord]{})

		router := &mockContentRouter{}
		router.On("GetClosestPeers", mock.Anything, key).Return(results, nil)

		resp := makeRequest(t, router, mediaTypeNDJSON, key.String())
		require.Equal(t, 200, resp.StatusCode)

		require.Equal(t, mediaTypeNDJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, "public, max-age=15, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

		requireCloseToNow(t, resp.Header.Get("Last-Modified"))
	})

	t.Run("GET /routing/v1/dht/closest/peers/{cid} returns 200 with correct body and headers (NDJSON)", func(t *testing.T) {
		t.Parallel()

		_, pid := makeEd25519PeerID(t)
		key := peer.ToCid(pid)
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
		router.On("GetClosestPeers", mock.Anything, key).Return(results, nil)

		resp := makeRequest(t, router, mediaTypeNDJSON, key.String())
		require.Equal(t, 200, resp.StatusCode)

		require.Equal(t, mediaTypeNDJSON, resp.Header.Get("Content-Type"))
		require.Equal(t, "Accept", resp.Header.Get("Vary"))
		require.Equal(t, "public, max-age=300, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		expectedBody := `{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-bitswap","transport-foo"],"Schema":"peer"}` + "\n" + `{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-foo"],"Schema":"peer"}` + "\n"
		require.Equal(t, expectedBody, string(body))
	})

	// Test matrix that runs the HTTP 200 scenario against different key formats.
	// The test verifies that GetClosestPeers is called with a CID whose digest matches the expected value,
	// regardless of the CID codec. This is correct because DHT operations only use the digest.
	// per https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation
	keyTestCases := []struct {
		keyType        string
		keyStr         string
		expectedDigest string // hex-encoded multihash digest
	}{
		// Examples from libp2p spec
		// https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation
		{"cidv1-libp2p-key-ed25519-peerid", "bafzbeie5745rpv2m6tjyuugywy4d5ewrqgqqhfnf445he3omzpjbx5xqxe", "12209dff3b17d74cf4d38a50d8b6383e92d181a10395a5e73a726dcccbd21bf6f0b9"},
		{"base58-ed25519-peerid", "12D3KooWD3eckifWpRn9wQpMG9R9hX3sD158z7EqHWmweQAJU5SA", "0024080112202ffa35a99d3a3cfbb17bb7c1dc5561b18a8dcca4df38dc613ea859c37eb1336b"},
		{"base58-rsa-peerid", "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N", "12209dff3b17d74cf4d38a50d8b6383e92d181a10395a5e73a726dcccbd21bf6f0b9"},
		// Arbitrary CID (not a PeerID)
		{"arbitrary-cid", "bafkreidcd7frenco2m6ch7mny63wztgztv3q6fctaffgowkro6kljre5ei", "1220621fcb12344ed33c23fd8dc7b76cccd99d770f1453014a6759517794b4c49d22"},
	}

	for _, tc := range keyTestCases {
		// Parse the key to get the actual digest
		parsedKey, err := parseKey(tc.keyStr)
		require.NoError(t, err)
		actualDigest := parsedKey.Hash()

		// Verify it matches expected
		require.Equal(t, tc.expectedDigest, actualDigest.HexString())

		// Create a PeerID from the digest for response records
		pid, err := peer.IDFromBytes(actualDigest)
		require.NoError(t, err)

		results := []iter.Result[*types.PeerRecord]{
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
		}

		t.Run("GET /routing/v1/dht/closest/peers/{"+tc.keyType+"} returns 200 with correct body and headers (JSON)", func(t *testing.T) {
			t.Parallel()

			router := &mockContentRouter{}
			// Use mock.MatchedBy to verify the digest matches, regardless of codec
			router.On("GetClosestPeers", mock.Anything, mock.MatchedBy(func(key cid.Cid) bool {
				return bytes.Equal(key.Hash(), actualDigest)
			})).Return(iter.FromSlice(results), nil)

			resp := makeRequest(t, router, mediaTypeJSON, tc.keyStr)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			require.Equal(t, mediaTypeJSON, resp.Header.Get("Content-Type"))
			require.Equal(t, "Accept", resp.Header.Get("Vary"))
			require.Equal(t, "public, max-age=300, stale-while-revalidate=172800, stale-if-error=172800", resp.Header.Get("Cache-Control"))

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			expectedBody := `{"Peers":[{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-bitswap","transport-foo"],"Schema":"peer"},{"Addrs":[],"ID":"` + pid.String() + `","Protocols":["transport-foo"],"Schema":"peer"}]}`
			require.Equal(t, expectedBody, string(body))
		})
	}
}

func makeName(t *testing.T) (crypto.PrivKey, ipns.Name) {
	sk, pid := makeEd25519PeerID(t)
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
			require.Equal(t, http.StatusOK, resp.StatusCode)
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
			require.Equal(t, http.StatusOK, resp.StatusCode)
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
			require.Equal(t, http.StatusOK, resp.StatusCode)
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

		t.Run("GET /routing/v1/ipns/{cid-peer-id} returns 200 with text/plain (no record found)", func(t *testing.T) {
			t.Parallel()

			router := &mockContentRouter{}
			router.On("GetIPNS", mock.Anything, name1).Return(nil, routing.ErrNotFound)

			// Simulate request without explicit Accept header
			noAccept := ""
			resp := makeRequest(t, router, "/routing/v1/ipns/"+name1.String(), noAccept)

			// Per IPIP-0513: Return 200 with text/plain for no record found
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("Content-Type"))

			// Verify error message in body
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Contains(t, string(body), "routing: not found")
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
			require.Equal(t, http.StatusOK, resp.StatusCode)
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

func (m *mockContentRouter) ProvideBitswap(ctx context.Context, req *BitswapWriteProvideRequest) (time.Duration, error) {
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

func (m *mockContentRouter) GetClosestPeers(ctx context.Context, key cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
	args := m.Called(ctx, key)
	a := args.Get(0)
	if a == nil {
		return nil, args.Error(1)
	}
	return a.(iter.ResultIter[*types.PeerRecord]), args.Error(1)
}
