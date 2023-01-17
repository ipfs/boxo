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
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/routing/http/server"
	"github.com/ipfs/go-libipfs/routing/http/types"
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

func (m *mockContentRouter) FindProviders(ctx context.Context, key cid.Cid) ([]types.ProviderResponse, error) {
	args := m.Called(ctx, key)
	return args.Get(0).([]types.ProviderResponse), args.Error(1)
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
	router *mockContentRouter
	server *httptest.Server
	peerID peer.ID
	addrs  []multiaddr.Multiaddr
	client *client
}

func makeTestDeps(t *testing.T) testDeps {
	const testUserAgent = "testUserAgent"
	peerID, addrs, identity := makeProviderAndIdentity()
	router := &mockContentRouter{}
	server := httptest.NewServer(server.Handler(router))
	t.Cleanup(server.Close)
	serverAddr := "http://" + server.Listener.Addr().String()
	c, err := New(serverAddr, WithProviderInfo(peerID, addrs), WithIdentity(identity), WithUserAgent(testUserAgent))
	if err != nil {
		panic(err)
	}
	assertUserAgentOverride(t, c, testUserAgent)
	return testDeps{
		router: router,
		server: server,
		peerID: peerID,
		addrs:  addrs,
		client: c,
	}
}

func assertUserAgentOverride(t *testing.T, c *client, expected string) {
	httpClient, ok := c.httpClient.(*http.Client)
	if !ok {
		t.Error("invalid c.httpClient")
	}
	transport, ok := httpClient.Transport.(*ResponseBodyLimitedTransport)
	if !ok {
		t.Error("invalid httpClient.Transport")
	}
	if transport.UserAgent != expected {
		t.Error("invalid httpClient.Transport.UserAgent")
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

func TestClient_FindProviders(t *testing.T) {
	bsReadProvResp := makeBSReadProviderResp()
	bitswapProvs := []types.ProviderResponse{&bsReadProvResp}

	cases := []struct {
		name           string
		httpStatusCode int
		stopServer     bool
		routerProvs    []types.ProviderResponse
		routerErr      error

		expProvs          []types.ProviderResponse
		expErrContains    []string
		expWinErrContains []string
	}{
		{
			name:        "happy case",
			routerProvs: bitswapProvs,
			expProvs:    bitswapProvs,
		},
		{
			name:           "returns an error if there's a non-200 response",
			httpStatusCode: 500,
			expErrContains: []string{"HTTP error with StatusCode=500: "},
		},
		{
			name:              "returns an error if the HTTP client returns a non-HTTP error",
			stopServer:        true,
			expErrContains:    []string{"connect: connection refused"},
			expWinErrContains: []string{"connectex: No connection could be made because the target machine actively refused it."},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			deps := makeTestDeps(t)
			client := deps.client
			router := deps.router

			if c.httpStatusCode != 0 {
				deps.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(c.httpStatusCode)
				})
			}

			if c.stopServer {
				deps.server.Close()
			}
			cid := makeCID()

			router.On("FindProviders", mock.Anything, cid).
				Return(c.routerProvs, c.routerErr)

			provs, err := client.FindProviders(context.Background(), cid)

			var errList []string
			if runtime.GOOS == "windows" && len(c.expWinErrContains) != 0 {
				errList = c.expWinErrContains
			} else {
				errList = c.expErrContains
			}

			for _, exp := range errList {
				require.ErrorContains(t, err, exp)
			}
			if len(errList) == 0 {
				require.NoError(t, err)
			}

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

			expErrContains: "HTTP error with StatusCode=403",
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
			//			deps := makeTestDeps(t)
			deps := makeTestDeps(t)
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
