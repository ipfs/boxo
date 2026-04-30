package httpnet

import (
	"errors"
	"sync"
)

var errThresholdCrossed = errors.New("the host crossed the error threshold")

// errorTracker counts client errors (e.g. 404) per HTTP endpoint.
//
// Counts are keyed on endpointKey, not on peer.ID. Multiple peer IDs
// that resolve to the same gateway contribute to one shared counter,
// so a misbehaving host trips the breaker once for all peers using it
// rather than after every peer drained its own quota.
type errorTracker struct {
	mu     sync.Mutex
	counts map[string]int
}

func newErrorTracker() *errorTracker {
	return &errorTracker{counts: make(map[string]int)}
}

// logErrors attributes n client errors to every host backing urls.
// When n is zero, the counters for those hosts reset (a successful
// SendMsg signals the hosts are healthy).
//
// Returns errThresholdCrossed when any host's count exceeds threshold.
// The caller decides what to do with that signal; today it disconnects
// the peer that triggered the check. Other peers using the same host
// are caught lazily on their next SendMsg.
func (et *errorTracker) logErrors(urls []*senderURL, n, threshold int) error {
	if len(urls) == 0 {
		return nil
	}
	et.mu.Lock()
	defer et.mu.Unlock()

	if n == 0 {
		for _, u := range urls {
			delete(et.counts, endpointKey(u.URL.Scheme, u.URL.Host, u.SNI))
		}
		return nil
	}

	var tripped bool
	for _, u := range urls {
		key := endpointKey(u.URL.Scheme, u.URL.Host, u.SNI)
		et.counts[key] += n
		if et.counts[key] > threshold {
			tripped = true
		}
	}
	if tripped {
		return errThresholdCrossed
	}
	return nil
}

// reset drops the count for key. Called when no peer is using the host
// so a future reconnect starts fresh.
func (et *errorTracker) reset(key string) {
	et.mu.Lock()
	delete(et.counts, key)
	et.mu.Unlock()
}
