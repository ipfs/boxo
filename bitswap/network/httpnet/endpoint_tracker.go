package httpnet

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/boxo/bitswap/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

var errThresholdCrossed = errors.New("the host crossed the error threshold")

// endpointState is the state shared by every peer ID whose addresses
// resolve to one HTTP endpoint (a unique (scheme, host, SNI) triple).
//
// Delegated routing routinely returns several peer IDs for one HTTP
// gateway. Without sharing, every (peer, URL) pair keeps its own error
// accounting, so a broken host drains N times the intended quota before
// every peer disconnects, and every extra peer ID re-probes an endpoint
// already proven working.
type endpointState struct {
	refcount    int           // connected peers registered against this endpoint
	probeMethod string        // method a Connect probe succeeded with; "" when never probed
	probeRTT    time.Duration // round trip of the probe that proved the endpoint

	// serverErrors is the shared breaker counter. senderURL holds a
	// pointer to it, so counting on the hot path does not go through
	// the tracker lock.
	serverErrors atomic.Int64

	clientErrors int // client errors (e.g. 404) accumulated by SendMsg

	// throttleStreak counts consecutive throttle responses that came
	// without a usable Retry-After header, so the fallback backoff can
	// grow instead of polling the host at a fixed short interval.
	throttleStreak int
}

// endpointProbe describes one endpoint of a connecting peer: its key,
// the method its Connect probe succeeded with, and the probe round trip.
// The method is empty when the endpoint was kept without a probe (an
// ongoing cooldown).
type endpointProbe struct {
	key    string
	method string
	rtt    time.Duration
}

// endpointTracker is the registry of per-endpoint shared state, keyed by
// endpointKey. Entries live while at least one connected peer is
// registered against them; when the last peer disconnects the state is
// forgotten, so the next connection starts fresh.
type endpointTracker struct {
	mu        sync.Mutex
	endpoints map[string]*endpointState
	peerKeys  map[peer.ID][]string
}

func newEndpointTracker() *endpointTracker {
	return &endpointTracker{
		endpoints: make(map[string]*endpointState),
		peerKeys:  make(map[peer.ID][]string),
	}
}

func (et *endpointTracker) getOrCreateLocked(key string) *endpointState {
	state, ok := et.endpoints[key]
	if !ok {
		state = &endpointState{}
		et.endpoints[key] = state
	}
	return state
}

// register records p as a user of the given endpoints, bumping their
// refcounts. Probes carrying a method (own probe or inherited from
// another peer) set the endpoint's proven method; the method persists
// while any peer holds the endpoint. Registering an already-registered
// peer is a no-op.
func (et *endpointTracker) register(p peer.ID, probes []endpointProbe) {
	if len(probes) == 0 {
		return
	}
	et.mu.Lock()
	defer et.mu.Unlock()

	if _, ok := et.peerKeys[p]; ok {
		return
	}

	keys := make([]string, 0, len(probes))
	for _, probe := range probes {
		state := et.getOrCreateLocked(probe.key)
		state.refcount++
		if state.probeMethod == "" {
			state.probeMethod = probe.method
		}
		if state.probeRTT == 0 {
			state.probeRTT = probe.rtt
		}
		keys = append(keys, probe.key)
	}
	et.peerKeys[p] = keys
}

// release unregisters p from its endpoints. Endpoints whose refcount
// drops to zero are forgotten.
func (et *endpointTracker) release(p peer.ID) {
	et.mu.Lock()
	defer et.mu.Unlock()

	for _, key := range et.peerKeys[p] {
		state, ok := et.endpoints[key]
		if !ok {
			continue
		}
		state.refcount--
		if state.refcount <= 0 {
			delete(et.endpoints, key)
		}
	}
	delete(et.peerKeys, p)
}

// knownMethod returns the probe method and probe round trip proven for
// the endpoint of u by a currently-connected peer, allowing Connect to
// skip the probe for the next peer ID resolving to the same gateway.
// Endpoints whose shared error counter reached maxServerErrors are not
// reported: a peer must not inherit a tripped breaker; Connect probes
// such endpoints for real and resets the counter on success.
func (et *endpointTracker) knownMethod(u network.ParsedURL, maxServerErrors int) (string, time.Duration, bool) {
	et.mu.Lock()
	defer et.mu.Unlock()

	state, ok := et.endpoints[endpointKey(u.URL.Scheme, u.URL.Host, u.SNI)]
	if !ok || state.refcount <= 0 || state.probeMethod == "" ||
		state.serverErrors.Load() >= int64(maxServerErrors) {
		return "", 0, false
	}
	return state.probeMethod, state.probeRTT, true
}

// clearServerErrors resets the shared breaker counter and throttle
// streak for key after a successful probe proved the endpoint healthy.
func (et *endpointTracker) clearServerErrors(key string) {
	et.mu.Lock()
	defer et.mu.Unlock()
	if state, ok := et.endpoints[key]; ok {
		state.serverErrors.Store(0)
		state.throttleStreak = 0
	}
}

// serverErrorCounter returns the shared breaker counter for key. When
// no connected peer holds the endpoint (a sender racing a disconnect),
// it returns a detached counter instead of resurrecting released state.
func (et *endpointTracker) serverErrorCounter(key string) *atomic.Int64 {
	et.mu.Lock()
	defer et.mu.Unlock()
	if state, ok := et.endpoints[key]; ok {
		return &state.serverErrors
	}
	return new(atomic.Int64)
}

// logClientErrors attributes n client errors to every endpoint backing
// urls, once per unique endpoint. When n is zero, the counters for
// those endpoints reset (a SendMsg round without client errors signals
// the hosts are healthy). Endpoints no longer registered are skipped,
// so a round finishing after a disconnect cannot resurrect released
// state.
//
// Returns errThresholdCrossed when any endpoint's count exceeds
// threshold. The caller decides what to do with that signal; today it
// disconnects the peer that triggered the check, and other peers using
// the same endpoint are caught on their next SendMsg.
func (et *endpointTracker) logClientErrors(urls []*senderURL, n, threshold int) error {
	if len(urls) == 0 {
		return nil
	}
	et.mu.Lock()
	defer et.mu.Unlock()

	var tripped bool
	seen := make(map[string]struct{}, len(urls))
	for _, u := range urls {
		key := endpointKey(u.URL.Scheme, u.URL.Host, u.SNI)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		state, ok := et.endpoints[key]
		if !ok {
			continue
		}
		if n == 0 {
			state.clientErrors = 0
			continue
		}
		state.clientErrors += n
		if state.clientErrors > threshold {
			tripped = true
		}
	}
	if tripped {
		return errThresholdCrossed
	}
	return nil
}

// nextThrottleBackoff returns how long to back off after a throttle
// response that carried no usable Retry-After. The wait starts at base
// and doubles per consecutive headerless throttle on the endpoint, up
// to max, so senders configured with short retry backoffs cannot poll
// a throttling host at a fixed short interval forever. The streak
// resets when the endpoint answers properly (clearThrottleStreak) or
// when its state is dropped.
func (et *endpointTracker) nextThrottleBackoff(key string, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = DefaultSendErrorBackoff
	}
	et.mu.Lock()
	defer et.mu.Unlock()
	state, ok := et.endpoints[key]
	if !ok {
		return base
	}
	d := base << min(state.throttleStreak, 20)
	if state.throttleStreak < 20 {
		state.throttleStreak++
	}
	if d <= 0 || d > max {
		d = max
	}
	return d
}

// clearThrottleStreak resets the headerless-throttle streak for key
// after the endpoint produced a definitive response.
func (et *endpointTracker) clearThrottleStreak(key string) {
	et.mu.Lock()
	defer et.mu.Unlock()
	if state, ok := et.endpoints[key]; ok {
		state.throttleStreak = 0
	}
}
