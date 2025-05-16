package httpnet

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/libp2p/go-libp2p/core/peer"
)

var errThresholdCrossed = errors.New("the peer crossed the error threshold")

type errorTracker struct {
	ht *Network

	mux    sync.RWMutex
	errors map[peer.ID]*atomic.Uint64
}

func newErrorTracker(ht *Network) *errorTracker {
	return &errorTracker{
		ht:     ht,
		errors: make(map[peer.ID]*atomic.Uint64),
	}
}

func (et *errorTracker) startTracking(p peer.ID) {
	et.mux.Lock()
	defer et.mux.Unlock()

	// Initialize the error count for the peer if it doesn't exist
	if _, exists := et.errors[p]; !exists {
		et.errors[p] = &atomic.Uint64{}
	}
}

func (et *errorTracker) stopTracking(p peer.ID) {
	et.mux.Lock()
	defer et.mux.Unlock()
	delete(et.errors, p)
}

// logErrors adds n to the current error count for p. If the total count is above the threshold, then an error is returned. If n is 0, the the total count is reset to 0.
func (et *errorTracker) logErrors(p peer.ID, n uint64, threshold uint64) error {
	et.mux.RLock()
	count, ok := et.errors[p]
	et.mux.RUnlock()

	if !ok {
		// i.e. we disconnected but there were pending requests
		log.Debug("logging errors for untracked peer: %s", p)
		return nil
	}

	if n == 0 { // reset error count
		count.Store(0)
		return nil
	}

	total := count.Add(n)
	if total > threshold {
		return errThresholdCrossed
	}
	return nil
}
