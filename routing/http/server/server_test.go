package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestHeaders(t *testing.T) {
	router := &mockContentRouter{}
	server := httptest.NewServer(Handler(router))
	t.Cleanup(server.Close)
	serverAddr := "http://" + server.Listener.Addr().String()

	results := iter.FromSlice([]iter.Result[types.ProviderResponse]{
		{Val: &types.ReadBitswapProviderRecord{
			Protocol: "transport-bitswap",
			Schema:   types.SchemaBitswap,
		}}},
	)

	c := "baeabep4vu3ceru7nerjjbk37sxb7wmftteve4hcosmyolsbsiubw2vr6pqzj6mw7kv6tbn6nqkkldnklbjgm5tzbi4hkpkled4xlcr7xz4bq"
	cb, err := cid.Decode(c)
	require.NoError(t, err)

	router.On("FindProviders", mock.Anything, cb, false).
		Return(results, nil)

	resp, err := http.Get(serverAddr + ProvidePath + c)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	header := resp.Header.Get("Content-Type")
	require.Equal(t, mediaTypeJSON, header)

	resp, err = http.Get(serverAddr + ProvidePath + "BAD_CID")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 400, resp.StatusCode)
	header = resp.Header.Get("Content-Type")
	require.Equal(t, "text/plain; charset=utf-8", header)
}

func TestResponse(t *testing.T) {
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

		results := iter.FromSlice([]iter.Result[types.ProviderResponse]{
			{Val: &types.ReadBitswapProviderRecord{
				Protocol: "transport-bitswap",
				Schema:   types.SchemaBitswap,
				ID:       &pid,
				Addrs:    []types.Multiaddr{},
			}},
			{Val: &types.ReadBitswapProviderRecord{
				Protocol: "transport-bitswap",
				Schema:   types.SchemaBitswap,
				ID:       &pid2,
				Addrs:    []types.Multiaddr{},
			}}},
		)

		router := &mockContentRouter{}
		server := httptest.NewServer(Handler(router))
		t.Cleanup(server.Close)
		serverAddr := "http://" + server.Listener.Addr().String()
		router.On("FindProviders", mock.Anything, cid, expectedStream).Return(results, nil)
		urlStr := serverAddr + ProvidePath + cidStr

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

		require.Equal(t, string(body), expectedBody)
	}

	t.Run("JSON Response", func(t *testing.T) {
		runTest(t, mediaTypeJSON, false, `{"Providers":[{"Protocol":"transport-bitswap","Schema":"bitswap","ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn","Addrs":[]},{"Protocol":"transport-bitswap","Schema":"bitswap","ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vz","Addrs":[]}]}`)
	})

	t.Run("NDJSON Response", func(t *testing.T) {
		runTest(t, mediaTypeNDJSON, true, `{"Protocol":"transport-bitswap","Schema":"bitswap","ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn","Addrs":[]}`+"\n"+`{"Protocol":"transport-bitswap","Schema":"bitswap","ID":"12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vz","Addrs":[]}`+"\n")
	})
}

type mockContentRouter struct{ mock.Mock }

func (m *mockContentRouter) FindProviders(ctx context.Context, key cid.Cid, stream bool) (iter.ResultIter[types.ProviderResponse], error) {
	args := m.Called(ctx, key, stream)
	return args.Get(0).(iter.ResultIter[types.ProviderResponse]), args.Error(1)
}
func (m *mockContentRouter) ProvideBitswap(ctx context.Context, req *BitswapWriteProvideRequest) (time.Duration, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(time.Duration), args.Error(1)
}

func (m *mockContentRouter) Provide(ctx context.Context, req *WriteProvideRequest) (types.ProviderResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(types.ProviderResponse), args.Error(1)
}
