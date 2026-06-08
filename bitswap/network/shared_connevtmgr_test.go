package network_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	"github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
	"github.com/ipfs/boxo/bitswap/network/httpnet"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/host"
	libp2pnet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// httpnet probes an endpoint by requesting this identity (empty raw block) CID;
// it must match the unexported pingCid in bitswap/network/httpnet.
const stubPingCid = "bafkqaaa"

// Note: this is mostly a sanity check. In practice a provider is reached either
// over libp2p (bitswap) or over HTTP, not both at once, so this exact
// interaction is unlikely. The test simulates a peer reachable both ways on
// purpose, to guard the shared connection-state abstraction and prevent
// regressions like https://github.com/ipfs/boxo/issues/1163.
//
// What this test checks:
//
//	A peer can be reachable two ways at once: over bitswap (libp2p) and over
//	HTTP. Both transports share one network.ConnectEventManager, which tracks
//	whether each peer is connected. The test sends a single bitswap message that
//	fails on the first try and succeeds on the retry. After that recoverable
//	failure the peer must still count as connected: the bitswap client must not
//	be told the peer disconnected, and HTTP must keep working.
//
// Why this test exists:
//
//	bsnet (bitswap) and httpnet share one ConnectEventManager because kubo sets
//	them up that way in core/node/bitswap.go. So one transport's view of a peer
//	affects the other. bsnet.send() used to mark a peer unresponsive after every
//	failed try. Because the manager is shared, a single failure told the bitswap
//	client the peer had disconnected, even though HTTP could still reach it. The
//	peer was then dropped with no way to come back, so fetches hung forever. That
//	extra mark was not needed: multiAttempt() already marks the peer once all
//	retries are used up.
//
//	  - the shared manager and the extra mark were added together: https://github.com/ipfs/boxo/pull/984
//	  - the extra mark made fetches hang after a reconnect:         https://github.com/ipfs/boxo/issues/1163
//	  - removing the extra mark fixed it:                           https://github.com/ipfs/boxo/pull/1164
//
// How the test sets this up:
//
//	One peer is reachable over both transports, sharing one manager:
//	  - bitswap: a real libp2p stream over mocknet; its first NewStream fails
//	  - HTTP:    a stub gateway (HTTP/2 + TLS), like the real HTTP providers
//	The test makes one bitswap send fail and then recover on the retry. With the
//	fix the peer stays connected. Without it, the shared manager wrongly reports
//	the peer as disconnected even though HTTP still works.
func TestSharedConnEvtMgr_TransientBitswapFailureKeepsHTTPPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	mn := mocknet.New()
	t.Cleanup(func() { _ = mn.Close() })

	clientHost, err := mn.GenPeer()
	require.NoError(t, err)
	serverHost, err := mn.GenPeer()
	require.NoError(t, err)
	require.NoError(t, mn.LinkAll())
	remote := serverHost.ID()

	// Fail the client's next bitswap stream open, simulating a stale stream
	// right after a reconnect.
	client := &flakyHost{Host: clientHost}

	// One ConnectEventManager shared by bsnet and httpnet, exactly like kubo.
	shared := network.NewConnectEventManager()
	bsLibp2p := bsnet.NewFromIpfsHost(client, bsnet.WithConnectEventManager(shared))
	bsHTTP := httpnet.New(clientHost, httpnet.WithConnectEventManager(shared), httpnet.WithInsecureSkipVerify(true))

	rec := newConnRecorder()
	// The first Start registers our listener on the shared manager and starts
	// it. The second reuses the same manager, so both transports report peer
	// connects and disconnects to the same listener.
	bsLibp2p.Start(rec)
	bsHTTP.Start(rec)
	t.Cleanup(bsLibp2p.Stop)
	t.Cleanup(bsHTTP.Stop)

	// Server side must answer the recovered bitswap stream.
	bsServer := bsnet.NewFromIpfsHost(serverHost)
	bsServer.Start(newConnRecorder())
	t.Cleanup(bsServer.Stop)

	gw := newStubGateway(t)

	// Make the peer reachable over both bitswap and HTTP.
	require.NoError(t, bsLibp2p.Connect(ctx, peer.AddrInfo{ID: remote}))
	require.NoError(t, bsHTTP.Connect(ctx, peer.AddrInfo{ID: remote, Addrs: []multiaddr.Multiaddr{gw}}))
	require.True(t, bsHTTP.IsConnectedToPeer(ctx, remote), "peer should be connected over HTTP after connecting")

	const backoff = 100 * time.Millisecond
	sender, err := bsLibp2p.NewMessageSender(ctx, remote, &network.MessageSenderOpts{
		MaxRetries:       3,
		SendTimeout:      100 * time.Millisecond,
		SendErrorBackoff: backoff,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sender.Reset() })

	// Drop the cached stream so the next send opens a new one and hits the
	// injected failure. Fail that open now, then clear the failure during the
	// backoff so the retry works. This is one recoverable failure.
	require.NoError(t, sender.Reset())
	client.setFailing(true)
	go func() {
		time.Sleep(backoff / 2)
		client.setFailing(false)
	}()

	// SendMsg recovers on the retry, so it returns no error.
	require.NoError(t, sender.SendMsg(ctx, blockWant(t)))

	// The peer must stay connected. The libp2p connection never dropped (only
	// one stream open failed), and HTTP was never touched, so any
	// PeerDisconnected here is the bug. Fail right away if one arrives;
	// otherwise wait a moment to be sure none is coming, then pass.
	select {
	case p := <-rec.disconnected:
		t.Fatalf("peer %s disconnected after a single recoverable bitswap send failure, "+
			"despite still being reachable over HTTP (regression of "+
			"https://github.com/ipfs/boxo/issues/1163)", p)
	case <-time.After(time.Second):
	}

	require.True(t, bsHTTP.IsConnectedToPeer(ctx, remote),
		"peer should still be connected over HTTP after a recoverable bitswap send failure")
}

// flakyHost wraps a libp2p host and fails NewStream while failing is set,
// simulating a transient bitswap stream failure.
type flakyHost struct {
	host.Host
	mu      sync.Mutex
	failing bool
}

func (h *flakyHost) setFailing(b bool) {
	h.mu.Lock()
	h.failing = b
	h.mu.Unlock()
}

func (h *flakyHost) NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (libp2pnet.Stream, error) {
	h.mu.Lock()
	failing := h.failing
	h.mu.Unlock()
	if failing {
		return nil, errors.New("injected bitswap NewStream failure")
	}
	return h.Host.NewStream(ctx, p, pids...)
}

// connRecorder is a minimal network.Receiver used as the shared manager's
// listener (standing in for the bitswap client). It reports disconnects on a
// channel so the test can assert that none occurs.
type connRecorder struct {
	disconnected chan peer.ID
}

func newConnRecorder() *connRecorder {
	return &connRecorder{disconnected: make(chan peer.ID, 8)}
}

func (r *connRecorder) ReceiveMessage(context.Context, peer.ID, bsmsg.BitSwapMessage) {}
func (r *connRecorder) ReceiveError(error)                                            {}
func (r *connRecorder) PeerConnected(peer.ID)                                         {}
func (r *connRecorder) PeerDisconnected(p peer.ID) {
	select {
	case r.disconnected <- p:
	default:
	}
}

// newStubGateway starts a stub trustless gateway that answers httpnet's probe,
// which is all httpnet.Connect needs to consider the peer reachable over HTTP.
// It serves HTTP/2 over TLS like real gateway providers do.
func newStubGateway(t *testing.T) multiaddr.Multiaddr {
	t.Helper()

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, stubPingCid) {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	base, err := manet.FromNetAddr(srv.Listener.Addr())
	require.NoError(t, err)
	https, err := multiaddr.NewMultiaddr("/https")
	require.NoError(t, err)
	return base.Encapsulate(https)
}

// blockWant builds a wantlist message for an arbitrary block.
func blockWant(t *testing.T) bsmsg.BitSwapMessage {
	t.Helper()
	mh, err := multihash.Sum([]byte("dual-transport-regression"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	m := bsmsg.New(false)
	m.AddEntry(cid.NewCidV1(cid.Raw, mh), 1, pb.Message_Wantlist_Block, true)
	return m
}
