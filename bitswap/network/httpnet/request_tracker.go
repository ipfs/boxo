package httpnet

import (
	"context"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
)

// requestTracker tracks requests to CIDs so that we can cancel all ongoing
// requests a single CID.
type requestTracker struct {
	clearTimeout time.Duration

	mux     sync.Mutex
	timers  map[cid.Cid]*time.Timer
	cancels map[cid.Cid][]context.CancelFunc
}

// newRequestTracker creates a new requestTracker with the given clearTimeout.
// All tracked requests to a given CID are cancelled after the given timeout.
// Normally, tracking should be cancelled using cancelRequest, but this serves
// as a way of cleaning up when that has not happened. The timers are reset when
// a new request to a CID happens.
func newRequestTracker(clearTimeout time.Duration) *requestTracker {
	return &requestTracker{
		clearTimeout: clearTimeout,
		timers:       make(map[cid.Cid]*time.Timer),
		cancels:      make(map[cid.Cid][]context.CancelFunc),
	}
}

// requestContext returns a new context to make a request for a cid. The
// context will be cancelled if cancelRequest() is called for the same CID.
func (rc *requestTracker) requestContext(ctx context.Context, c cid.Cid) context.Context {
	cctx, cancel := context.WithCancel(ctx)
	rc.mux.Lock()
	defer rc.mux.Unlock()
	if rc.cancels == nil {
		rc.cancels = make(map[cid.Cid][]context.CancelFunc)
		rc.timers = make(map[cid.Cid]*time.Timer)
	}

	cancels, ok := rc.cancels[c]
	if !ok {
		rc.cancels[c] = []context.CancelFunc{cancel}
		rc.timers[c] = time.AfterFunc(rc.clearTimeout, func() {
			rc.cancelRequest(c)
		})
		return cctx
	}
	rc.cancels[c] = append(cancels, cancel)
	rc.timers[c].Reset(rc.clearTimeout)

	return cctx
}

// cancelRequest cancels all contexts obtained via requestContext for the
// given CID.
func (rc *requestTracker) cancelRequest(c cid.Cid) {
	rc.mux.Lock()
	defer rc.mux.Unlock()
	t, ok := rc.timers[c]
	if ok {
		t.Stop()
	}

	for _, cancel := range rc.cancels[c] {
		cancel()
	}
	delete(rc.cancels, c)
	delete(rc.timers, c)
}
