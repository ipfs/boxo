package client

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/ipfs/boxo/coreiface/path"
	ipns "github.com/ipfs/boxo/ipns"
	ipfspath "github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/routing/http/server"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockContentRouter struct{ mock.Mock }

func (m *mockContentRouter) GetProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	args := m.Called(ctx, key, limit)
	return args.Get(0).(iter.ResultIter[types.Record]), args.Error(1)
}

func (m *mockContentRouter) GetIPNSRecord(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	args := m.Called(ctx, name)
	return args.Get(0).(*ipns.Record), args.Error(1)
}

func (m *mockContentRouter) PutIPNSRecord(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	args := m.Called(ctx, name, record)
	return args.Error(0)
}

type testDeps struct {
	// recordingHandler records requests received on the server side
	recordingHandler *recordingHandler
	// recordingHTTPClient records responses received on the client side
	recordingHTTPClient *recordingHTTPClient
	router              *mockContentRouter
	server              *httptest.Server
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

func makeBSReadProviderResp() types.PeerRecord {
	peerID, addrs, _ := makeProviderAndIdentity()
	return types.PeerRecord{
		Schema:    types.SchemaPeer,
		ID:        &peerID,
		Protocols: []string{"transport-bitswap"},
		Addrs:     addrsToDRAddrs(addrs),
		Extra:     map[string]json.RawMessage{},
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

func TestClient_GetProviders(t *testing.T) {
	bsReadProvResp := makeBSReadProviderResp()
	bitswapProvs := []iter.Result[types.Record]{
		{Val: &bsReadProvResp},
	}

	cases := []struct {
		name                    string
		httpStatusCode          int
		stopServer              bool
		routerProvs             []iter.Result[types.Record]
		routerErr               error
		clientRequiresStreaming bool
		serverStreamingDisabled bool

		expErrContains       osErrContains
		expProvs             []iter.Result[types.Record]
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
				router.On("GetProviders", mock.Anything, cid, 0).Return(findProvsIter, c.routerErr)
			} else {
				router.On("GetProviders", mock.Anything, cid, 20).Return(findProvsIter, c.routerErr)
			}

			provsIter, err := client.GetProviders(ctx, cid)

			c.expErrContains.errContains(t, err)

			provs := iter.ReadAll[iter.Result[types.Record]](provsIter)
			assert.Equal(t, c.expProvs, provs)
		})
	}
}

func makeName(t *testing.T) (crypto.PrivKey, ipns.Name) {
	sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	pid, err := peer.IDFromPrivateKey(sk)
	require.NoError(t, err)

	return sk, ipns.NameFromPeer(pid)
}

func makeIPNSRecord(t *testing.T, sk crypto.PrivKey, opts ...ipns.Option) (*ipns.Record, []byte) {
	cid, err := cid.Decode("bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	require.NoError(t, err)

	path := path.IpfsPath(cid)
	eol := time.Now().Add(time.Hour * 48)
	ttl := time.Second * 20

	record, err := ipns.NewRecord(sk, ipfspath.FromString(path.String()), 1, eol, ttl, opts...)
	require.NoError(t, err)

	rawRecord, err := ipns.MarshalRecord(record)
	require.NoError(t, err)

	return record, rawRecord
}

func TestClient_IPNS(t *testing.T) {
	t.Run("Find IPNS Record returns error if server errors", func(t *testing.T) {
		_, name := makeName(t)

		deps := makeTestDeps(t, nil, nil)
		client := deps.client
		router := deps.router

		router.On("GetIPNSRecord", mock.Anything, name).Return(nil, errors.New("something wrong happened"))

		receivedRecord, err := client.GetIPNSRecord(context.Background(), name)
		require.Error(t, err)
		require.Nil(t, receivedRecord)
	})

	runWithRecordOptions := func(t *testing.T, opts ...ipns.Option) {
		t.Run("Find IPNS Record", func(t *testing.T) {
			sk, name := makeName(t)
			record, _ := makeIPNSRecord(t, sk, opts...)

			deps := makeTestDeps(t, nil, nil)
			client := deps.client
			router := deps.router

			router.On("GetIPNSRecord", mock.Anything, name).Return(record, nil)

			receivedRecord, err := client.GetIPNSRecord(context.Background(), name)
			require.NoError(t, err)
			require.Equal(t, record, receivedRecord)
		})

		t.Run("Find IPNS Record returns error if server sends bad data", func(t *testing.T) {
			sk, _ := makeName(t)
			record, _ := makeIPNSRecord(t, sk, opts...)
			_, name2 := makeName(t)

			deps := makeTestDeps(t, nil, nil)
			client := deps.client
			router := deps.router

			router.On("GetIPNSRecord", mock.Anything, name2).Return(record, nil)

			receivedRecord, err := client.GetIPNSRecord(context.Background(), name2)
			require.Error(t, err)
			require.Nil(t, receivedRecord)
		})

		t.Run("Provide IPNS Record", func(t *testing.T) {
			sk, name := makeName(t)
			record, _ := makeIPNSRecord(t, sk, opts...)

			deps := makeTestDeps(t, nil, nil)
			client := deps.client
			router := deps.router

			router.On("PutIPNSRecord", mock.Anything, name, record).Return(nil)

			err := client.PutIPNSRecord(context.Background(), name, record)
			require.NoError(t, err)
		})
	}

	t.Run("V1+V2 IPNS Records", func(t *testing.T) {
		runWithRecordOptions(t, ipns.WithV1Compatibility(true))
	})

	t.Run("V2 IPNS Records", func(t *testing.T) {
		runWithRecordOptions(t, ipns.WithV1Compatibility(false))
	})
}
