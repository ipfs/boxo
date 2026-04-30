package httpnet

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ipfs/boxo/bitswap/network"
)

// TestPingerSharesHostsAcrossPeers verifies that two peer IDs which
// resolve to the same HTTP endpoint share one host entry (refcount 2)
// rather than producing two independent tickers.
func TestPingerSharesHostsAcrossPeers(t *testing.T) {
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

	// One server, two peers pointing at it.
	srv := makeServer(t, 0, 0)
	mustConnectToPeer(t, ctx, htnet, peerA, srv)
	mustConnectToPeer(t, ctx, htnet, peerB, srv)

	htnet.pinger.mu.Lock()
	hosts := len(htnet.pinger.hosts)
	var refcount int
	for _, hp := range htnet.pinger.hosts {
		refcount = hp.refcount
	}
	htnet.pinger.mu.Unlock()

	if hosts != 1 {
		t.Fatalf("got %d host entries, want 1", hosts)
	}
	if refcount != 2 {
		t.Fatalf("got refcount %d, want 2", refcount)
	}
	if !htnet.pinger.isPinging(peerA.ID()) {
		t.Errorf("peerA should be pinging")
	}
	if !htnet.pinger.isPinging(peerB.ID()) {
		t.Errorf("peerB should be pinging")
	}
}

// TestPingerStopReleasesRefcount verifies that disconnecting one of two
// peers sharing a host leaves the host entry alive (refcount 1) and
// that disconnecting the second drops it.
func TestPingerStopReleasesRefcount(t *testing.T) {
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

	srv := makeServer(t, 0, 0)
	mustConnectToPeer(t, ctx, htnet, peerA, srv)
	mustConnectToPeer(t, ctx, htnet, peerB, srv)

	if err := htnet.DisconnectFrom(ctx, peerA.ID()); err != nil {
		t.Fatal(err)
	}

	htnet.pinger.mu.Lock()
	hostsAfterA := len(htnet.pinger.hosts)
	var refcountAfterA int
	for _, hp := range htnet.pinger.hosts {
		refcountAfterA = hp.refcount
	}
	htnet.pinger.mu.Unlock()

	if hostsAfterA != 1 {
		t.Fatalf("after disconnecting peerA: got %d host entries, want 1", hostsAfterA)
	}
	if refcountAfterA != 1 {
		t.Fatalf("after disconnecting peerA: got refcount %d, want 1", refcountAfterA)
	}

	if err := htnet.DisconnectFrom(ctx, peerB.ID()); err != nil {
		t.Fatal(err)
	}

	htnet.pinger.mu.Lock()
	hostsAfterB := len(htnet.pinger.hosts)
	htnet.pinger.mu.Unlock()

	if hostsAfterB != 0 {
		t.Fatalf("after disconnecting peerB: got %d host entries, want 0", hostsAfterB)
	}
}

// TestPingerCleansSharedStateOnLastDisconnect verifies that when the
// last peer using a host disconnects, the host-shared breaker and
// errorTracker entries are dropped so a future reconnect starts fresh.
func TestPingerCleansSharedStateOnLastDisconnect(t *testing.T) {
	ctx := context.Background()

	htnet, mn := mockNetwork(t, mockReceiver(t))
	peerA, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	srv := makeServer(t, 0, 0)
	mustConnectToPeer(t, ctx, htnet, peerA, srv)

	// Seed breaker and errorTracker state for the host.
	urls := htnet.senderURLs(peerA.ID())
	if len(urls) != 1 {
		t.Fatalf("expected 1 senderURL, got %d", len(urls))
	}
	urls[0].serverErrors.Store(7)
	if err := htnet.errorTracker.logErrors(urls, 5, 100); err != nil {
		t.Fatalf("errorTracker.logErrors: %v", err)
	}

	if err := htnet.DisconnectFrom(ctx, peerA.ID()); err != nil {
		t.Fatal(err)
	}

	// Last peer is gone; both shared structures should have dropped
	// their entries.
	htnet.breaker.mu.Lock()
	breakerEntries := len(htnet.breaker.counters)
	htnet.breaker.mu.Unlock()
	if breakerEntries != 0 {
		t.Errorf("breaker still has %d entries after last disconnect", breakerEntries)
	}

	htnet.errorTracker.mu.Lock()
	errorEntries := len(htnet.errorTracker.counts)
	htnet.errorTracker.mu.Unlock()
	if errorEntries != 0 {
		t.Errorf("errorTracker still has %d entries after last disconnect", errorEntries)
	}
}

// TestPingerSkipsProbeOnCooldown verifies that ping() honours
// Retry-After: when the cooldownTracker shows the host is in its
// wait window, no HTTP probe is issued and the per-URL goroutine
// returns errCooldownActive.
func TestPingerSkipsProbeOnCooldown(t *testing.T) {
	ctx := context.Background()

	htnet, mn := mockNetwork(t, mockReceiver(t))
	peerA, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	srv, probes := countingProbeServer(t)
	mustConnectToPeer(t, ctx, htnet, peerA, srv)

	// Connect probed once; baseline.
	if got := probes.Load(); got != 1 {
		t.Fatalf("after Connect: got %d probes, want 1", got)
	}

	// Put the host on cooldown directly. ping() should skip the probe.
	host := htnet.host.Peerstore().Addrs(peerA.ID())[0]
	purl, err := network.ExtractHTTPAddress(host)
	if err != nil {
		t.Fatal(err)
	}
	htnet.cooldownTracker.setByDuration(purl.URL.Host, 30*time.Second)

	res := htnet.pinger.ping(ctx, peerA.ID())
	if res.Error == nil || !errors.Is(res.Error, errCooldownActive) {
		t.Fatalf("ping during cooldown: got err %v, want errCooldownActive", res.Error)
	}
	if got := probes.Load(); got != 1 {
		t.Errorf("after Ping during cooldown: got %d probes, want 1 (no new probe)", got)
	}
}

// TestPingerDistinctHostsRunIndependently verifies that two peers with
// different HTTP endpoints each get their own host entry.
func TestPingerDistinctHostsRunIndependently(t *testing.T) {
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

	srvA := makeServer(t, 0, 0)
	srvB := makeServer(t, 0, 0)
	mustConnectToPeer(t, ctx, htnet, peerA, srvA)
	mustConnectToPeer(t, ctx, htnet, peerB, srvB)

	htnet.pinger.mu.Lock()
	hosts := len(htnet.pinger.hosts)
	htnet.pinger.mu.Unlock()

	if hosts != 2 {
		t.Fatalf("got %d host entries, want 2", hosts)
	}
}
