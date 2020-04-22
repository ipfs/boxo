package messagequeue

import (
	"context"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

const (
	// dontHaveTimeout is used to simulate a DONT_HAVE when communicating with
	// a peer whose Bitswap client doesn't support the DONT_HAVE response,
	// or when the peer takes too long to respond.
	// If the peer doesn't respond to a want-block within the timeout, the
	// local node assumes that the peer doesn't have the block.
	dontHaveTimeout = 5 * time.Second

	// maxExpectedWantProcessTime is the maximum amount of time we expect a
	// peer takes to process a want and initiate sending a response to us
	maxExpectedWantProcessTime = 2 * time.Second

	// latencyMultiplier is multiplied by the average ping time to
	// get an upper bound on how long we expect to wait for a peer's response
	// to arrive
	latencyMultiplier = 3
)

// PeerConnection is a connection to a peer that can be pinged, and the
// average latency measured
type PeerConnection interface {
	// Ping the peer
	Ping(context.Context) ping.Result
	// The average latency of all pings
	Latency() time.Duration
}

// pendingWant keeps track of a want that has been sent and we're waiting
// for a response or for a timeout to expire
type pendingWant struct {
	c      cid.Cid
	active bool
	sent   time.Time
}

// dontHaveTimeoutMgr pings the peer to measure latency. It uses the latency to
// set a reasonable timeout for simulating a DONT_HAVE message for peers that
// don't support DONT_HAVE or that take to long to respond.
type dontHaveTimeoutMgr struct {
	ctx                        context.Context
	shutdown                   func()
	peerConn                   PeerConnection
	onDontHaveTimeout          func([]cid.Cid)
	defaultTimeout             time.Duration
	latencyMultiplier          int
	maxExpectedWantProcessTime time.Duration

	// All variables below here must be protected by the lock
	lk sync.RWMutex
	// has the timeout manager started
	started bool
	// wants that are active (waiting for a response or timeout)
	activeWants map[cid.Cid]*pendingWant
	// queue of wants, from oldest to newest
	wantQueue []*pendingWant
	// time to wait for a response (depends on latency)
	timeout time.Duration
	// timer used to wait until want at front of queue expires
	checkForTimeoutsTimer *time.Timer
}

// newDontHaveTimeoutMgr creates a new dontHaveTimeoutMgr
// onDontHaveTimeout is called when pending keys expire (not cancelled before timeout)
func newDontHaveTimeoutMgr(pc PeerConnection, onDontHaveTimeout func([]cid.Cid)) *dontHaveTimeoutMgr {
	return newDontHaveTimeoutMgrWithParams(pc, onDontHaveTimeout, dontHaveTimeout,
		latencyMultiplier, maxExpectedWantProcessTime)
}

// newDontHaveTimeoutMgrWithParams is used by the tests
func newDontHaveTimeoutMgrWithParams(pc PeerConnection, onDontHaveTimeout func([]cid.Cid),
	defaultTimeout time.Duration, latencyMultiplier int,
	maxExpectedWantProcessTime time.Duration) *dontHaveTimeoutMgr {

	ctx, shutdown := context.WithCancel(context.Background())
	mqp := &dontHaveTimeoutMgr{
		ctx:                        ctx,
		shutdown:                   shutdown,
		peerConn:                   pc,
		activeWants:                make(map[cid.Cid]*pendingWant),
		timeout:                    defaultTimeout,
		defaultTimeout:             defaultTimeout,
		latencyMultiplier:          latencyMultiplier,
		maxExpectedWantProcessTime: maxExpectedWantProcessTime,
		onDontHaveTimeout:          onDontHaveTimeout,
	}

	return mqp
}

// Shutdown the dontHaveTimeoutMgr. Any subsequent call to Start() will be ignored
func (dhtm *dontHaveTimeoutMgr) Shutdown() {
	dhtm.shutdown()

	dhtm.lk.Lock()
	defer dhtm.lk.Unlock()

	// Clear any pending check for timeouts
	if dhtm.checkForTimeoutsTimer != nil {
		dhtm.checkForTimeoutsTimer.Stop()
	}
}

// Start the dontHaveTimeoutMgr. This method is idempotent
func (dhtm *dontHaveTimeoutMgr) Start() {
	dhtm.lk.Lock()
	defer dhtm.lk.Unlock()

	// Make sure the dont have timeout manager hasn't already been started
	if dhtm.started {
		return
	}
	dhtm.started = true

	// If we already have a measure of latency to the peer, use it to
	// calculate a reasonable timeout
	latency := dhtm.peerConn.Latency()
	if latency.Nanoseconds() > 0 {
		dhtm.timeout = dhtm.calculateTimeoutFromLatency(latency)
		return
	}

	// Otherwise measure latency by pinging the peer
	go dhtm.measureLatency()
}

// measureLatency measures the latency to the peer by pinging it
func (dhtm *dontHaveTimeoutMgr) measureLatency() {
	// Wait up to defaultTimeout for a response to the ping
	ctx, cancel := context.WithTimeout(dhtm.ctx, dhtm.defaultTimeout)
	defer cancel()

	// Ping the peer
	res := dhtm.peerConn.Ping(ctx)
	if res.Error != nil {
		// If there was an error, we'll just leave the timeout as
		// defaultTimeout
		return
	}

	// Get the average latency to the peer
	latency := dhtm.peerConn.Latency()

	dhtm.lk.Lock()
	defer dhtm.lk.Unlock()

	// Calculate a reasonable timeout based on latency
	dhtm.timeout = dhtm.calculateTimeoutFromLatency(latency)

	// Check if after changing the timeout there are any pending wants that are
	// now over the timeout
	dhtm.checkForTimeouts()
}

// checkForTimeouts checks pending wants to see if any are over the timeout.
// Note: this function should only be called within the lock.
func (dhtm *dontHaveTimeoutMgr) checkForTimeouts() {
	if len(dhtm.wantQueue) == 0 {
		return
	}

	// Figure out which of the blocks that were wanted were not received
	// within the timeout
	expired := make([]cid.Cid, 0, len(dhtm.activeWants))
	for len(dhtm.wantQueue) > 0 {
		pw := dhtm.wantQueue[0]

		// If the want is still active
		if pw.active {
			// The queue is in order from earliest to latest, so if we
			// didn't find an expired entry we can stop iterating
			if time.Since(pw.sent) < dhtm.timeout {
				break
			}

			// Add the want to the expired list
			expired = append(expired, pw.c)
			// Remove the want from the activeWants map
			delete(dhtm.activeWants, pw.c)
		}

		// Remove expired or cancelled wants from the want queue
		dhtm.wantQueue = dhtm.wantQueue[1:]
	}

	// Fire the timeout event for the expired wants
	if len(expired) > 0 {
		go dhtm.fireTimeout(expired)
	}

	if len(dhtm.wantQueue) == 0 {
		return
	}

	// Make sure the timeout manager is still running
	if dhtm.ctx.Err() != nil {
		return
	}

	// Schedule the next check for the moment when the oldest pending want will
	// timeout
	oldestStart := dhtm.wantQueue[0].sent
	until := time.Until(oldestStart.Add(dhtm.timeout))
	if dhtm.checkForTimeoutsTimer == nil {
		dhtm.checkForTimeoutsTimer = time.AfterFunc(until, func() {
			dhtm.lk.Lock()
			defer dhtm.lk.Unlock()

			dhtm.checkForTimeouts()
		})
	} else {
		dhtm.checkForTimeoutsTimer.Stop()
		dhtm.checkForTimeoutsTimer.Reset(until)
	}
}

// AddPending adds the given keys that will expire if not cancelled before
// the timeout
func (dhtm *dontHaveTimeoutMgr) AddPending(ks []cid.Cid) {
	if len(ks) == 0 {
		return
	}

	start := time.Now()

	dhtm.lk.Lock()
	defer dhtm.lk.Unlock()

	queueWasEmpty := len(dhtm.activeWants) == 0

	// Record the start time for each key
	for _, c := range ks {
		if _, ok := dhtm.activeWants[c]; !ok {
			pw := pendingWant{
				c:      c,
				sent:   start,
				active: true,
			}
			dhtm.activeWants[c] = &pw
			dhtm.wantQueue = append(dhtm.wantQueue, &pw)
		}
	}

	// If there was already an earlier pending item in the queue, then there
	// must already be a timeout check scheduled. If there is nothing in the
	// queue then we should make sure to schedule a check.
	if queueWasEmpty {
		dhtm.checkForTimeouts()
	}
}

// CancelPending is called when we receive a response for a key
func (dhtm *dontHaveTimeoutMgr) CancelPending(ks []cid.Cid) {
	dhtm.lk.Lock()
	defer dhtm.lk.Unlock()

	// Mark the wants as cancelled
	for _, c := range ks {
		if pw, ok := dhtm.activeWants[c]; ok {
			pw.active = false
			delete(dhtm.activeWants, c)
		}
	}
}

// fireTimeout fires the onDontHaveTimeout method with the timed out keys
func (dhtm *dontHaveTimeoutMgr) fireTimeout(pending []cid.Cid) {
	// Make sure the timeout manager has not been shut down
	if dhtm.ctx.Err() != nil {
		return
	}

	// Fire the timeout
	dhtm.onDontHaveTimeout(pending)
}

// calculateTimeoutFromLatency calculates a reasonable timeout derived from latency
func (dhtm *dontHaveTimeoutMgr) calculateTimeoutFromLatency(latency time.Duration) time.Duration {
	// The maximum expected time for a response is
	// the expected time to process the want + (latency * multiplier)
	// The multiplier is to provide some padding for variable latency.
	return dhtm.maxExpectedWantProcessTime + time.Duration(dhtm.latencyMultiplier)*latency
}
