package client

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/boxo/routing/http/server"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockContentRouter struct{ mock.Mock }

func (m *mockContentRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.ProviderResponse], error) {
	args := m.Called(ctx, key, limit)
	return args.Get(0).(iter.ResultIter[types.ProviderResponse]), args.Error(1)
}

func (m *mockContentRouter) ProvideBitswap(ctx context.Context, req *server.BitswapWriteProvideRequest) (time.Duration, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(time.Duration), args.Error(1)
}

func (m *mockContentRouter) Provide(ctx context.Context, req *server.WriteProvideRequest) (types.ProviderResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(types.ProviderResponse), args.Error(1)
}

type testDeps struct {
	// recordingHandler records requests received on the server side
	recordingHandler *recordingHandler
	// recordingHTTPClient records responses received on the client side
	recordingHTTPClient *recordingHTTPClient
	router              *mockContentRouter
	server              *httptest.Server
	peerID              peer.ID
	addrs               []multiaddr.Multiaddr
	client              *client
}

type recordingHandler struct {
	http.Handler
	f []func(*http.Request)
}

func (h *recordingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for _, f := range h.f {
		f(r)
	}
	h.Handler.ServeHTTP(w, r)
}

type recordingHTTPClient struct {
	httpClient
	f []func(*http.Response)
}

func (c *recordingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.httpClient.Do(req)
	for _, f := range c.f {
		f(resp)
	}
	return resp, err
}

func makeTestDeps(t *testing.T, clientsOpts []Option, serverOpts []server.Option) testDeps {
	const testUserAgent = "testUserAgent"
	peerID, addrs, identity := makeProviderAndIdentity()
	router := &mockContentRouter{}
	recordingHandler := &recordingHandler{
		Handler: server.Handler(router, serverOpts...),
		f: []func(*http.Request){
			func(r *http.Request) {
				assert.Equal(t, testUserAgent, r.Header.Get("User-Agent"))
			},
		},
	}
	server := httptest.NewServer(recordingHandler)
	t.Cleanup(server.Close)
	serverAddr := "http://" + server.Listener.Addr().String()
	recordingHTTPClient := &recordingHTTPClient{httpClient: defaultHTTPClient}
	defaultClientOpts := []Option{
		WithProviderInfo(peerID, addrs),
		WithIdentity(identity),
		WithUserAgent(testUserAgent),
		WithHTTPClient(recordingHTTPClient),
	}
	c, err := New(serverAddr, append(defaultClientOpts, clientsOpts...)...)
	if err != nil {
		panic(err)
	}
	return testDeps{
		recordingHandler:    recordingHandler,
		recordingHTTPClient: recordingHTTPClient,
		router:              router,
		server:              server,
		peerID:              peerID,
		addrs:               addrs,
		client:              c,
	}
}

func makeCID() cid.Cid {
	buf := make([]byte, 63)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err)
	}
	mh, err := multihash.Encode(buf, multihash.SHA2_256)
	if err != nil {
		panic(err)
	}
	c := cid.NewCidV1(0, mh)
	return c
}

func addrsToDRAddrs(addrs []multiaddr.Multiaddr) (drmas []types.Multiaddr) {
	for _, a := range addrs {
		drmas = append(drmas, types.Multiaddr{Multiaddr: a})
	}
	return
}

func drAddrsToAddrs(drmas []types.Multiaddr) (addrs []multiaddr.Multiaddr) {
	for _, a := range drmas {
		addrs = append(addrs, a.Multiaddr)
	}
	return
}

func makeBSReadProviderResp() types.ReadBitswapProviderRecord {
	peerID, addrs, _ := makeProviderAndIdentity()
	return types.ReadBitswapProviderRecord{
		Protocol: "transport-bitswap",
		Schema:   types.SchemaBitswap,
		ID:       &peerID,
		Addrs:    addrsToDRAddrs(addrs),
	}
}

func makeProviderAndIdentity() (peer.ID, []multiaddr.Multiaddr, crypto.PrivKey) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		panic(err)
	}
	peerID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		panic(err)
	}
	ma1, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/4001")
	if err != nil {
		panic(err)
	}

	ma2, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/4002")
	if err != nil {
		panic(err)
	}

	return peerID, []multiaddr.Multiaddr{ma1, ma2}, priv
}

type osErrContains struct {
	expContains    string
	expContainsWin string
}

func (e *osErrContains) errContains(t *testing.T, err error) {
	if e.expContains == "" && e.expContainsWin == "" {
		assert.NoError(t, err)
		return
	}
	if runtime.GOOS == "windows" && len(e.expContainsWin) != 0 {
		assert.ErrorContains(t, err, e.expContainsWin)
	} else {
		assert.ErrorContains(t, err, e.expContains)
	}
}

func TestClient_FindProviders(t *testing.T) {
	bsReadProvResp := makeBSReadProviderResp()
	bitswapProvs := []iter.Result[types.ProviderResponse]{
		{Val: &bsReadProvResp},
	}

	cases := []struct {
		name                    string
		httpStatusCode          int
		stopServer              bool
		routerProvs             []iter.Result[types.ProviderResponse]
		routerErr               error
		clientRequiresStreaming bool
		serverStreamingDisabled bool

		expErrContains       osErrContains
		expProvs             []iter.Result[types.ProviderResponse]
		expStreamingResponse bool
		expJSONResponse      bool
	}{
		{
			name:                 "happy case",
			routerProvs:          bitswapProvs,
			expProvs:             bitswapProvs,
			expStreamingResponse: true,
		},
		{
			name:                    "server doesn't support streaming",
			routerProvs:             bitswapProvs,
			expProvs:                bitswapProvs,
			serverStreamingDisabled: true,
			expJSONResponse:         true,
		},
		{
			name:                    "client requires streaming but server doesn't support it",
			serverStreamingDisabled: true,
			clientRequiresStreaming: true,
			expErrContains:          osErrContains{expContains: "HTTP error with StatusCode=400: no supported content types"},
		},
		{
			name:           "returns an error if there's a non-200 response",
			httpStatusCode: 500,
			expErrContains: osErrContains{expContains: "HTTP error with StatusCode=500"},
		},
		{
			name:       "returns an error if the HTTP client returns a non-HTTP error",
			stopServer: true,
			expErrContains: osErrContains{
				expContains:    "connect: connection refused",
				expContainsWin: "connectex: No connection could be made because the target machine actively refused it.",
			},
		},
		{
			name:           "returns no providers if the HTTP server returns a 404 respones",
			httpStatusCode: 404,
			expProvs:       nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var clientOpts []Option
			var serverOpts []server.Option
			var onRespReceived []func(*http.Response)
			var onReqReceived []func(*http.Request)

			if c.serverStreamingDisabled {
				serverOpts = append(serverOpts, server.WithStreamingResultsDisabled())
			}
			if c.clientRequiresStreaming {
				clientOpts = append(clientOpts, WithStreamResultsRequired())
				onReqReceived = append(onReqReceived, func(r *http.Request) {
					assert.Equal(t, mediaTypeNDJSON, r.Header.Get("Accept"))
				})
			}

			if c.expStreamingResponse {
				onRespReceived = append(onRespReceived, func(r *http.Response) {
					assert.Equal(t, mediaTypeNDJSON, r.Header.Get("Content-Type"))
				})
			}
			if c.expJSONResponse {
				onRespReceived = append(onRespReceived, func(r *http.Response) {
					assert.Equal(t, mediaTypeJSON, r.Header.Get("Content-Type"))
				})
			}

			deps := makeTestDeps(t, clientOpts, serverOpts)

			deps.recordingHTTPClient.f = append(deps.recordingHTTPClient.f, onRespReceived...)
			deps.recordingHandler.f = append(deps.recordingHandler.f, onReqReceived...)

			client := deps.client
			router := deps.router

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			if c.httpStatusCode != 0 {
				deps.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(c.httpStatusCode)
				})
			}

			if c.stopServer {
				deps.server.Close()
			}
			cid := makeCID()

			findProvsIter := iter.FromSlice(c.routerProvs)

			if c.expStreamingResponse {
				router.On("FindProviders", mock.Anything, cid, 0).Return(findProvsIter, c.routerErr)
			} else {
				router.On("FindProviders", mock.Anything, cid, 20).Return(findProvsIter, c.routerErr)
			}

			provsIter, err := client.FindProviders(ctx, cid)

			c.expErrContains.errContains(t, err)

			provs := iter.ReadAll[iter.Result[types.ProviderResponse]](provsIter)
			assert.Equal(t, c.expProvs, provs)
		})
	}
}

func TestClient_Provide(t *testing.T) {
	cases := []struct {
		name            string
		manglePath      bool
		mangleSignature bool
		stopServer      bool
		noProviderInfo  bool
		noIdentity      bool

		cids []cid.Cid
		ttl  time.Duration

		routerAdvisoryTTL time.Duration
		routerErr         error

		expErrContains    string
		expWinErrContains string

		expAdvisoryTTL time.Duration
	}{
		{
			name:              "happy case",
			cids:              []cid.Cid{makeCID()},
			ttl:               1 * time.Hour,
			routerAdvisoryTTL: 1 * time.Minute,

			expAdvisoryTTL: 1 * time.Minute,
		},
		{
			name:            "should return a 403 if the payload signature verification fails",
			cids:            []cid.Cid{},
			mangleSignature: true,
			expErrContains:  "HTTP error with StatusCode=403",
		},
		{
			name:           "should return error if identity is not provided",
			noIdentity:     true,
			expErrContains: "cannot provide Bitswap records without an identity",
		},
		{
			name:           "should return error if provider is not provided",
			noProviderInfo: true,
			expErrContains: "cannot provide Bitswap records without a peer ID",
		},
		{
			name:           "returns an error if there's a non-200 response",
			manglePath:     true,
			expErrContains: "HTTP error with StatusCode=404: 404 page not found",
		},
		{
			name:              "returns an error if the HTTP client returns a non-HTTP error",
			stopServer:        true,
			expErrContains:    "connect: connection refused",
			expWinErrContains: "connectex: No connection could be made because the target machine actively refused it.",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			deps := makeTestDeps(t, nil, nil)
			client := deps.client
			router := deps.router

			if c.noIdentity {
				client.identity = nil
			}
			if c.noProviderInfo {
				client.peerID = ""
				client.addrs = nil
			}

			clock := clock.NewMock()
			clock.Set(time.Now())
			client.clock = clock

			ctx := context.Background()

			if c.manglePath {
				client.baseURL += "/foo"
			}
			if c.stopServer {
				deps.server.Close()
			}
			if c.mangleSignature {
				client.afterSignCallback = func(req *types.WriteBitswapProviderRecord) {
					mh, err := multihash.Encode([]byte("boom"), multihash.SHA2_256)
					require.NoError(t, err)
					mb, err := multibase.Encode(multibase.Base64, mh)
					require.NoError(t, err)

					req.Signature = mb
				}
			}

			expectedProvReq := &server.BitswapWriteProvideRequest{
				Keys:        c.cids,
				Timestamp:   clock.Now().Truncate(time.Millisecond),
				AdvisoryTTL: c.ttl,
				Addrs:       drAddrsToAddrs(client.addrs),
				ID:          client.peerID,
			}

			router.On("ProvideBitswap", mock.Anything, expectedProvReq).
				Return(c.routerAdvisoryTTL, c.routerErr)

			advisoryTTL, err := client.ProvideBitswap(ctx, c.cids, c.ttl)

			var errorString string
			if runtime.GOOS == "windows" && c.expWinErrContains != "" {
				errorString = c.expWinErrContains
			} else {
				errorString = c.expErrContains
			}

			if errorString != "" {
				require.ErrorContains(t, err, errorString)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, c.expAdvisoryTTL, advisoryTTL)
		})
	}
}
