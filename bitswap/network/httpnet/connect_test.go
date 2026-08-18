package httpnet

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ipfs/boxo/bitswap/network"
)

// countingProbeServer wraps the standard test handler and counts probe
// requests (any request to /ipfs/<pingCid>). The returned Handler allows
// overriding probe responses.
func countingProbeServer(t *testing.T) (srv *httptest.Server, handler *Handler, probes *atomic.Int32) {
	t.Helper()
	probes = new(atomic.Int32)
	handler = &Handler{bstore: makeBlockstore(t, 0, 0)}
	wrapped := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/ipfs/"+pingCid) {
			probes.Add(1)
		}
		handler.ServeHTTP(rw, r)
	})
	srv = httptest.NewUnstartedServer(wrapped)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, handler, probes
}

// TestConnectSkipsProbeForKnownEndpoint verifies that a second peer
// resolving to the same HTTP endpoint as a previously-connected peer
// avoids issuing a fresh probe.
//
// This is the expected pattern when delegated routing returns multiple
// peer IDs for one gateway.
func TestConnectSkipsProbeForKnownEndpoint(t *testing.T) {
	ctx := context.Background()

	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv)
	peerA, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	peerB, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	srv, _, probes := countingProbeServer(t)

	mustConnectToPeer(t, ctx, htnet, peerA, srv)
	if err := recv.waitConnected(1); err != nil {
		t.Fatal(err)
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("after peerA connect: got %d probes, want 1", got)
	}

	mustConnectToPeer(t, ctx, htnet, peerB, srv)
	if err := recv.waitConnected(1); err != nil {
		t.Fatal(err)
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("after peerB connect: got %d probes, want 1 (probe should be deduped)", got)
	}

	if !htnet.IsConnectedToPeer(ctx, peerB.ID()) {
		t.Errorf("peerB should be marked connected after probe dedup")
	}

	// No probe ran for peerB, but it inherits the endpoint's probe
	// round trip so the DONT_HAVE timeout manager sees a measured peer
	// and does not fire an on-demand ping.
	if lat := htnet.Latency(peerB.ID()); lat == 0 {
		t.Errorf("peerB should inherit the endpoint's probe latency, got 0")
	}
}

// TestConnectInheritsHEADSupport verifies that the second peer inherits
// the probed method from the cached endpoint state. The server rejects
// HEAD probes, so peerA is probed down to GET; peerB must inherit the
// GET decision (supportsHave false) without probing at all. A reverted
// inheritance would either re-probe (probe count changes) or default
// supportsHave to true.
func TestConnectInheritsHEADSupport(t *testing.T) {
	ctx := context.Background()

	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv)
	peerA, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	peerB, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	srv, handler, probes := countingProbeServer(t)
	handler.probeHeadStatus.Store(http.StatusMethodNotAllowed)

	mustConnectToPeer(t, ctx, htnet, peerA, srv)
	if err := recv.waitConnected(1); err != nil {
		t.Fatal(err)
	}
	if supportsHave(htnet.host.Peerstore(), peerA.ID()) {
		t.Fatal("peerA must not support HEAD (the server rejects HEAD probes)")
	}
	if got := probes.Load(); got != 2 {
		t.Fatalf("peerA should have probed HEAD then GET: got %d probes", got)
	}

	mustConnectToPeer(t, ctx, htnet, peerB, srv)
	if err := recv.waitConnected(1); err != nil {
		t.Fatal(err)
	}
	if got := probes.Load(); got != 2 {
		t.Fatalf("peerB should not probe at all: got %d probes, want 2", got)
	}
	if supportsHave(htnet.host.Peerstore(), peerB.ID()) {
		t.Errorf("peerB should inherit the GET-only decision from peerA's probe")
	}
}

// TestConnectReprobesTrippedEndpoint verifies that an endpoint whose
// shared breaker counter tripped is not inherited: the next peer ID
// probes it for real, and a successful probe forgives the accrued
// errors so everyone starts fresh.
func TestConnectReprobesTrippedEndpoint(t *testing.T) {
	ctx := context.Background()

	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv)
	peerA, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	peerB, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	srv, _, probes := countingProbeServer(t)
	mustConnectToPeer(t, ctx, htnet, peerA, srv)
	if err := recv.waitConnected(1); err != nil {
		t.Fatal(err)
	}

	urls := network.ExtractURLsFromPeer(htnet.host.Peerstore().PeerInfo(peerA.ID()))
	if len(urls) == 0 {
		t.Fatal("expected at least one URL on peerA")
	}
	key := endpointKey(urls[0].URL.Scheme, urls[0].URL.Host, urls[0].SNI)
	htnet.endpoints.serverErrorCounter(key).Store(int64(DefaultMaxRetries))

	mustConnectToPeer(t, ctx, htnet, peerB, srv)
	if err := recv.waitConnected(1); err != nil {
		t.Fatal(err)
	}
	if got := probes.Load(); got != 2 {
		t.Fatalf("tripped endpoint should be re-probed: got %d probes, want 2", got)
	}
	if got := htnet.endpoints.serverErrorCounter(key).Load(); got != 0 {
		t.Errorf("successful probe should forgive server errors, counter = %d", got)
	}
}

// TestConnectProbesNewEndpoint verifies that a peer pointing at an
// endpoint not yet in the cache still triggers a fresh probe.
func TestConnectProbesNewEndpoint(t *testing.T) {
	ctx := context.Background()

	recv := mockReceiver(t)
	htnet, mn := mockNetwork(t, recv)
	peerA, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	peerB, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	srvA, _, probesA := countingProbeServer(t)
	srvB, _, probesB := countingProbeServer(t)

	mustConnectToPeer(t, ctx, htnet, peerA, srvA)
	if err := recv.waitConnected(1); err != nil {
		t.Fatal(err)
	}
	mustConnectToPeer(t, ctx, htnet, peerB, srvB)
	if err := recv.waitConnected(1); err != nil {
		t.Fatal(err)
	}

	if got := probesA.Load(); got != 1 {
		t.Errorf("server A: got %d probes, want 1", got)
	}
	if got := probesB.Load(); got != 1 {
		t.Errorf("server B: got %d probes, want 1 (different endpoint, fresh probe expected)", got)
	}
}
