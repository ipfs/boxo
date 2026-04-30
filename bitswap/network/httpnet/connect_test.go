package httpnet

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// countingProbeServer wraps the standard test handler and counts probe
// requests (any request to /ipfs/<pingCid>).
func countingProbeServer(t *testing.T) (srv *httptest.Server, probes *atomic.Int32) {
	t.Helper()
	probes = new(atomic.Int32)
	inner := &Handler{bstore: makeBlockstore(t, 0, 0)}
	wrapped := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/ipfs/"+pingCid) {
			probes.Add(1)
		}
		inner.ServeHTTP(rw, r)
	})
	srv = httptest.NewUnstartedServer(wrapped)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, probes
}

// TestConnectSkipsProbeForKnownEndpoint verifies that a second peer
// resolving to the same HTTP endpoint as a previously-connected peer
// avoids issuing a fresh probe.
//
// This is the expected pattern when delegated routing returns multiple
// peer IDs for one gateway, e.g. /dns/a-fil-http.aur.lu/tcp/443/https
// is currently advertised under three peer IDs.
func TestConnectSkipsProbeForKnownEndpoint(t *testing.T) {
	ctx := context.Background()

	htnet, mn := mockNetwork(t, mockReceiver(t))
	peerA, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	peerB, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	srv, probes := countingProbeServer(t)

	mustConnectToPeer(t, ctx, htnet, peerA, srv)
	if got := probes.Load(); got != 1 {
		t.Fatalf("after peerA connect: got %d probes, want 1", got)
	}

	mustConnectToPeer(t, ctx, htnet, peerB, srv)
	if got := probes.Load(); got != 1 {
		t.Fatalf("after peerB connect: got %d probes, want 1 (probe should be deduped)", got)
	}

	if !htnet.IsConnectedToPeer(ctx, peerB.ID()) {
		t.Errorf("peerB should be marked connected after probe dedup")
	}
}

// TestConnectInheritsHEADSupport verifies that the second peer inherits
// the HEAD-support decision from the cached endpoint state.
func TestConnectInheritsHEADSupport(t *testing.T) {
	ctx := context.Background()

	htnet, mn := mockNetwork(t, mockReceiver(t))
	peerA, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	peerB, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	srv, _ := countingProbeServer(t)
	mustConnectToPeer(t, ctx, htnet, peerA, srv)
	if !supportsHave(htnet.host.Peerstore(), peerA.ID()) {
		t.Fatal("peerA should support HEAD (the test server answers HEAD)")
	}

	mustConnectToPeer(t, ctx, htnet, peerB, srv)
	if !supportsHave(htnet.host.Peerstore(), peerB.ID()) {
		t.Errorf("peerB should inherit HEAD support from peerA's cached probe")
	}
}

// TestConnectProbesNewEndpoint verifies that a peer pointing at an
// endpoint not yet in the cache still triggers a fresh probe.
func TestConnectProbesNewEndpoint(t *testing.T) {
	ctx := context.Background()

	htnet, mn := mockNetwork(t, mockReceiver(t))
	peerA, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	peerB, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	srvA, probesA := countingProbeServer(t)
	srvB, probesB := countingProbeServer(t)

	mustConnectToPeer(t, ctx, htnet, peerA, srvA)
	mustConnectToPeer(t, ctx, htnet, peerB, srvB)

	if got := probesA.Load(); got != 1 {
		t.Errorf("server A: got %d probes, want 1", got)
	}
	if got := probesB.Load(); got != 1 {
		t.Errorf("server B: got %d probes, want 1 (different endpoint, fresh probe expected)", got)
	}
}
