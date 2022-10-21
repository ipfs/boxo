package client

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	delegatedrouting "github.com/ipfs/go-delegated-routing"
	"github.com/ipfs/go-delegated-routing/server"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockContentRouter struct{ mock.Mock }

func (m *mockContentRouter) FindProviders(ctx context.Context, key cid.Cid) ([]delegatedrouting.Provider, error) {
	args := m.Called(ctx, key)
	return args.Get(0).([]delegatedrouting.Provider), args.Error(1)
}
func (m *mockContentRouter) Provide(ctx context.Context, req server.ProvideRequest) (time.Duration, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(time.Duration), args.Error(1)
}
func (m *mockContentRouter) Ready() bool {
	args := m.Called()
	return args.Bool(0)
}

type testDeps struct {
	router   *mockContentRouter
	server   *httptest.Server
	provider delegatedrouting.Provider
	client   *Client
}

func makeTestDeps(t *testing.T) testDeps {
	router := &mockContentRouter{}
	server := httptest.NewServer(server.Handler(router))
	t.Cleanup(server.Close)
	serverAddr := "http://" + server.Listener.Addr().String()
	provider := delegatedrouting.Provider{}
	c, err := New(serverAddr, &http.Client{}, provider)
	if err != nil {
		panic(err)
	}
	return testDeps{
		router:   router,
		server:   server,
		provider: provider,
		client:   c,
	}
}

func TestClient_Ready(t *testing.T) {
	cases := []struct {
		name           string
		manglePath     bool
		stopServer     bool
		routerReady    bool
		expStatus      bool
		expErrContains string
	}{
		{
			name:        "happy case",
			routerReady: true,
			expStatus:   true,
		},
		{
			name:        "503 returns false",
			routerReady: false,
			expStatus:   false,
		},
		{
			name:           "non-503 error returns an error",
			manglePath:     true,
			expStatus:      false,
			expErrContains: "unexpected HTTP status code '404'",
		},
		{
			name:           "undialable returns an error",
			stopServer:     true,
			expStatus:      false,
			expErrContains: "connect: connection refused",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			deps := makeTestDeps(t)
			client := deps.client
			router := deps.router

			if c.manglePath {
				client.baseURL += "/foo"
			}
			if c.stopServer {
				deps.server.Close()
			}

			router.On("Ready").Return(c.routerReady)

			ready, err := client.Ready(context.Background())

			if c.expErrContains != "" {
				assert.ErrorContains(t, err, c.expErrContains)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, c.expStatus, ready)
		})
	}
}

func makeProvider(protocols []delegatedrouting.TransferProtocol) delegatedrouting.Provider {
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

	return delegatedrouting.Provider{
		PeerID:    peerID,
		Addrs:     []multiaddr.Multiaddr{ma1, ma2},
		Protocols: protocols,
	}
}

func provsToAIs(provs []delegatedrouting.Provider) (ais []peer.AddrInfo) {
	for _, prov := range provs {
		ais = append(ais, peer.AddrInfo{
			ID:    prov.PeerID,
			Addrs: prov.Addrs,
		})
	}
	return
}

func TestClient_FindProviders(t *testing.T) {
	mh, err := multihash.Encode([]byte("asdf"), multihash.SHA2_256)
	if err != nil {
		panic(err)
	}
	cid := cid.NewCidV1(0, mh)

	bitswapProtocol := []delegatedrouting.TransferProtocol{{Codec: multicodec.TransportBitswap, Payload: []byte(`{"a":1}`)}}
	bitswapProvs := []delegatedrouting.Provider{makeProvider(bitswapProtocol), makeProvider(bitswapProtocol)}

	nonBitswapProtocol := []delegatedrouting.TransferProtocol{{Codec: multicodec.TransportGraphsyncFilecoinv1}}
	mixedProvs := []delegatedrouting.Provider{
		makeProvider(bitswapProtocol),
		makeProvider(nonBitswapProtocol),
	}

	cases := []struct {
		name        string
		manglePath  bool
		stopServer  bool
		routerProvs []delegatedrouting.Provider
		routerErr   error

		expAIs         []peer.AddrInfo
		expErrContains string
	}{
		{
			name:        "happy case",
			routerProvs: bitswapProvs,
			expAIs:      provsToAIs(bitswapProvs),
		},
		{
			name:        "non-bitswap providers are filtered by the client",
			routerProvs: mixedProvs,
			expAIs:      provsToAIs(mixedProvs[0:1]),
		},
		{
			name:           "returns an error if there's a non-200 response",
			manglePath:     true,
			expErrContains: "HTTP error with StatusCode=404: 404 page not found",
		},
		{
			name:           "returns an error if the HTTP client returns a non-HTTP error",
			stopServer:     true,
			expErrContains: "connect: connection refused",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			deps := makeTestDeps(t)
			client := deps.client
			router := deps.router

			if c.manglePath {
				client.baseURL += "/foo"
			}
			if c.stopServer {
				deps.server.Close()
			}

			router.On("FindProviders", mock.Anything, cid).
				Return(c.routerProvs, c.routerErr)

			ais, err := client.FindProviders(context.Background(), cid)

			if c.expErrContains != "" {
				require.ErrorContains(t, err, c.expErrContains)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, c.expAIs, ais)
		})
	}
}
