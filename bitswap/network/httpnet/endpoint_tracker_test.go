package httpnet

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/bitswap/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

func testParsedURL(t *testing.T, raw string) network.ParsedURL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return network.ParsedURL{URL: u}
}

func testSenderURL(t *testing.T, raw string) *senderURL {
	t.Helper()
	su := &senderURL{ParsedURL: testParsedURL(t, raw)}
	return su
}

func testPeer(t *testing.T) peer.ID {
	t.Helper()
	p, err := test.RandPeerID()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestEndpointTrackerSharedCounter(t *testing.T) {
	et := newEndpointTracker()
	p := testPeer(t)
	et.register(p, []endpointProbe{
		{key: "k", method: http.MethodHead},
		{key: "other", method: http.MethodHead},
	})

	c1 := et.serverErrorCounter("k")
	c2 := et.serverErrorCounter("k")
	if c1 != c2 {
		t.Fatal("same key should return the same counter")
	}
	if other := et.serverErrorCounter("other"); other == c1 {
		t.Fatal("distinct keys should have independent counters")
	}

	c1.Add(2)
	if got := et.serverErrorCounter("k").Load(); got != 2 {
		t.Fatalf("counter for key = %d, want 2", got)
	}

	// Unregistered endpoints get a detached counter: no shared state is
	// created behind the registry's back.
	d1 := et.serverErrorCounter("unregistered")
	d1.Add(5)
	if got := et.serverErrorCounter("unregistered").Load(); got != 0 {
		t.Fatalf("detached counter leaked into the registry: %d", got)
	}
}

func TestEndpointTrackerReleaseDropsState(t *testing.T) {
	et := newEndpointTracker()
	pA := testPeer(t)
	pB := testPeer(t)

	probes := []endpointProbe{{key: "k", method: http.MethodHead}}
	et.register(pA, probes)
	et.register(pB, probes)

	et.serverErrorCounter("k").Add(3)

	// pA leaves; pB still holds the endpoint, state survives.
	et.release(pA)
	if got := et.serverErrorCounter("k").Load(); got != 3 {
		t.Fatalf("counter dropped while a peer still uses the endpoint: %d", got)
	}
	et.mu.Lock()
	state := et.endpoints["k"]
	et.mu.Unlock()
	if state == nil || state.probeMethod != http.MethodHead {
		t.Fatal("probe method dropped while a peer still uses the endpoint")
	}

	// Last peer leaves; state is forgotten and a fresh connection
	// starts clean.
	et.release(pB)
	if got := et.serverErrorCounter("k").Load(); got != 0 {
		t.Fatalf("counter not reset after last release: %d", got)
	}
}

func TestEndpointTrackerRegisterIdempotent(t *testing.T) {
	et := newEndpointTracker()
	p := testPeer(t)

	probes := []endpointProbe{{key: "k", method: http.MethodHead}}
	et.register(p, probes)
	et.register(p, probes) // double Connect must not double-count

	et.release(p)
	et.mu.Lock()
	_, ok := et.endpoints["k"]
	et.mu.Unlock()
	if ok {
		t.Fatal("state should be dropped after single release of a doubly-registered peer")
	}
}

func TestEndpointTrackerKnownMethod(t *testing.T) {
	et := newEndpointTracker()
	p := testPeer(t)

	u := testParsedURL(t, "https://gateway.example.net")
	key := endpointKey(u.URL.Scheme, u.URL.Host, u.SNI)

	if _, _, ok := et.knownMethod(u, DefaultMaxRetries); ok {
		t.Fatal("unknown endpoint should not report a method")
	}

	// A cooled (never probed) endpoint registers without a method and
	// must not report one.
	et.register(p, []endpointProbe{{key: key}})
	if _, _, ok := et.knownMethod(u, DefaultMaxRetries); ok {
		t.Fatal("unprobed endpoint should not report a method")
	}
	et.release(p)

	et.register(p, []endpointProbe{{key: key, method: http.MethodGet, rtt: 30 * time.Millisecond}})
	method, rtt, ok := et.knownMethod(u, DefaultMaxRetries)
	if !ok || method != http.MethodGet {
		t.Fatalf("knownMethod = %q, %v; want GET, true", method, ok)
	}
	if rtt != 30*time.Millisecond {
		t.Fatalf("knownMethod rtt = %s, want 30ms", rtt)
	}

	// A tripped breaker must not be inherited: the endpoint needs a
	// real probe, and a successful one forgives the errors.
	et.serverErrorCounter(key).Store(int64(DefaultMaxRetries))
	if _, _, ok := et.knownMethod(u, DefaultMaxRetries); ok {
		t.Fatal("endpoint with a tripped breaker should not be inherited")
	}
	et.clearServerErrors(key)
	if _, _, ok := et.knownMethod(u, DefaultMaxRetries); !ok {
		t.Fatal("endpoint should be inheritable again after clearServerErrors")
	}

	et.release(p)
	if _, _, ok := et.knownMethod(u, DefaultMaxRetries); ok {
		t.Fatal("released endpoint should not report a method")
	}
}

func TestEndpointTrackerLogClientErrors(t *testing.T) {
	et := newEndpointTracker()

	urlsA := []*senderURL{testSenderURL(t, "https://gw.example.net")}
	urlsB := []*senderURL{testSenderURL(t, "https://gw.example.net")} // same endpoint
	other := []*senderURL{testSenderURL(t, "https://other.example.net")}

	// Counts only accumulate for registered (connected) endpoints.
	keyOf := func(u *senderURL) string {
		return endpointKey(u.URL.Scheme, u.URL.Host, u.SNI)
	}
	et.register(testPeer(t), []endpointProbe{
		{key: keyOf(urlsA[0]), method: http.MethodHead},
		{key: keyOf(other[0]), method: http.MethodHead},
	})

	threshold := 3

	// Counts accumulate across peers/senders that share the endpoint.
	if err := et.logClientErrors(urlsA, 2, threshold); err != nil {
		t.Fatalf("unexpected threshold trip: %v", err)
	}
	if err := et.logClientErrors(urlsB, 2, threshold); !errors.Is(err, errThresholdCrossed) {
		t.Fatalf("combined count 4 > 3 should trip, got %v", err)
	}

	// Distinct endpoints do not share counts.
	if err := et.logClientErrors(other, 2, threshold); err != nil {
		t.Fatalf("unexpected threshold trip for other endpoint: %v", err)
	}

	// A clean round resets the count.
	if err := et.logClientErrors(urlsA, 0, threshold); err != nil {
		t.Fatal(err)
	}
	if err := et.logClientErrors(urlsB, threshold, threshold); err != nil {
		t.Fatalf("count should have been reset, got %v", err)
	}

	// Duplicate URLs pointing at one endpoint charge it once, and
	// unregistered endpoints are skipped entirely.
	et2 := newEndpointTracker()
	dupKey := keyOf(urlsA[0])
	et2.register(testPeer(t), []endpointProbe{{key: dupKey, method: http.MethodHead}})
	dup := []*senderURL{testSenderURL(t, "https://gw.example.net"), testSenderURL(t, "https://gw.example.net")}
	if err := et2.logClientErrors(dup, 2, threshold); err != nil {
		t.Fatalf("duplicate URLs double-charged the endpoint: %v", err)
	}
	unregistered := []*senderURL{testSenderURL(t, "https://ghost.example.net")}
	if err := et2.logClientErrors(unregistered, threshold+1, threshold); err != nil {
		t.Fatalf("unregistered endpoint should not accumulate errors: %v", err)
	}
}

func TestEndpointTrackerThrottleBackoff(t *testing.T) {
	et := newEndpointTracker()
	base := 100 * time.Millisecond
	max := time.Second

	// Unregistered endpoints do not escalate.
	if d := et.nextThrottleBackoff("ghost", base, max); d != base {
		t.Fatalf("unregistered endpoint backoff = %s, want %s", d, base)
	}

	p := testPeer(t)
	et.register(p, []endpointProbe{{key: "k", method: http.MethodHead}})

	// Consecutive headerless throttles double the wait up to max.
	want := []time.Duration{base, 2 * base, 4 * base, 8 * base, max, max}
	for i, w := range want {
		if d := et.nextThrottleBackoff("k", base, max); d != w {
			t.Fatalf("throttle %d: backoff = %s, want %s", i, d, w)
		}
	}

	// A definitive response resets the streak.
	et.clearThrottleStreak("k")
	if d := et.nextThrottleBackoff("k", base, max); d != base {
		t.Fatalf("backoff after reset = %s, want %s", d, base)
	}
}

func TestEndpointTrackerConcurrency(t *testing.T) {
	et := newEndpointTracker()

	peers := make([]peer.ID, 8)
	for i := range peers {
		peers[i] = testPeer(t)
	}
	u := testParsedURL(t, "https://gw.example.net")

	var wg sync.WaitGroup
	for i, p := range peers {
		wg.Add(1)
		go func(i int, p peer.ID) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i%2)
			for range 100 {
				et.register(p, []endpointProbe{{key: key, method: http.MethodHead}})
				et.serverErrorCounter(key).Add(1)
				et.knownMethod(u, DefaultMaxRetries)
				et.release(p)
			}
		}(i, p)
	}
	wg.Wait()
}
