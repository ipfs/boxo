package httpnet

import (
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/boxo/bitswap/network"
)

// makeTestURLs returns n senderURLs each pointing at a distinct synthetic
// host. Counters and cooldowns are initialized so the senderURLs are
// safe to use against the per-host trackers.
func makeTestURLs(t *testing.T, hosts ...string) []*senderURL {
	t.Helper()
	out := make([]*senderURL, len(hosts))
	for i, h := range hosts {
		u, err := url.Parse("https://" + h)
		if err != nil {
			t.Fatalf("parse %q: %v", h, err)
		}
		su := &senderURL{
			ParsedURL:    network.ParsedURL{URL: u},
			serverErrors: new(atomic.Int64),
		}
		su.cooldown.Store(time.Time{})
		out[i] = su
	}
	return out
}

func TestErrorTracker_LogErrors_Reset(t *testing.T) {
	et := newErrorTracker()
	urls := makeTestURLs(t, "host-a")

	if err := et.logErrors(urls, 5, 10); err != nil {
		t.Errorf("logging 5 below threshold: got err %v, want nil", err)
	}

	if err := et.logErrors(urls, 0, 10); err != nil {
		t.Errorf("reset: got err %v, want nil", err)
	}

	key := endpointKey(urls[0].URL.Scheme, urls[0].URL.Host, urls[0].SNI)
	if got := et.counts[key]; got != 0 {
		t.Errorf("after reset: count=%d, want 0", got)
	}
}

func TestErrorTracker_LogErrors_ThresholdCrossed(t *testing.T) {
	et := newErrorTracker()
	urls := makeTestURLs(t, "host-a")

	if err := et.logErrors(urls, 11, 10); err != errThresholdCrossed {
		t.Errorf("logging above threshold: got err %v, want errThresholdCrossed", err)
	}

	key := endpointKey(urls[0].URL.Scheme, urls[0].URL.Host, urls[0].SNI)
	if got := et.counts[key]; got != 11 {
		t.Errorf("after trip: count=%d, want 11", got)
	}
}

// TestErrorTracker_SharesCounterAcrossPeers verifies that two peers
// resolving to the same HTTP endpoint share one error counter, so the
// breaker trips after one combined threshold rather than after each
// peer drains its own quota.
func TestErrorTracker_SharesCounterAcrossPeers(t *testing.T) {
	et := newErrorTracker()
	peerA := makeTestURLs(t, "shared-host")
	peerB := makeTestURLs(t, "shared-host")

	// Peer A logs 60 errors; threshold 100 not yet crossed.
	if err := et.logErrors(peerA, 60, 100); err != nil {
		t.Errorf("peerA: got err %v, want nil", err)
	}
	// Peer B logs 50 errors; combined 110 trips the breaker.
	if err := et.logErrors(peerB, 50, 100); err != errThresholdCrossed {
		t.Errorf("peerB: got err %v, want errThresholdCrossed", err)
	}
}

func TestErrorTracker_DistinctHostsTrackIndependently(t *testing.T) {
	et := newErrorTracker()
	a := makeTestURLs(t, "host-a")
	b := makeTestURLs(t, "host-b")

	if err := et.logErrors(a, 11, 10); err != errThresholdCrossed {
		t.Errorf("host-a: got err %v, want errThresholdCrossed", err)
	}
	if err := et.logErrors(b, 5, 10); err != nil {
		t.Errorf("host-b: got err %v, want nil (independent of host-a)", err)
	}
}

// TestErrorTracker_ConcurrentAccess verifies safety under concurrent
// goroutines incrementing the same host counter.
func TestErrorTracker_ConcurrentAccess(t *testing.T) {
	et := newErrorTracker()
	urls := makeTestURLs(t, "host-a")

	var wg sync.WaitGroup
	const numRoutines = 10
	const threshold = 100

	for range numRoutines {
		wg.Go(func() {
			for range threshold / numRoutines {
				et.logErrors(urls, 1, threshold)
			}
		})
	}
	wg.Wait()

	key := endpointKey(urls[0].URL.Scheme, urls[0].URL.Host, urls[0].SNI)
	if got := et.counts[key]; got != threshold {
		t.Errorf("after concurrent logging: count=%d, want %d", got, threshold)
	}
}
