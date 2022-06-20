package messagequeue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-bitswap/internal/testutil"
	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

type mockPeerConn struct {
	err       error
	latency   time.Duration
	latencies []time.Duration
	clock     clock.Clock
	pinged    chan struct{}
}

func (pc *mockPeerConn) Ping(ctx context.Context) ping.Result {
	timer := pc.clock.Timer(pc.latency)
	pc.pinged <- struct{}{}
	select {
	case <-timer.C:
		if pc.err != nil {
			return ping.Result{Error: pc.err}
		}
		pc.latencies = append(pc.latencies, pc.latency)
	case <-ctx.Done():
	}
	return ping.Result{RTT: pc.latency}
}

func (pc *mockPeerConn) Latency() time.Duration {
	sum := time.Duration(0)
	if len(pc.latencies) == 0 {
		return sum
	}
	for _, l := range pc.latencies {
		sum += l
	}
	return sum / time.Duration(len(pc.latencies))
}

type timeoutRecorder struct {
	timedOutKs []cid.Cid
	lk         sync.Mutex
}

func (tr *timeoutRecorder) onTimeout(tks []cid.Cid) {
	tr.lk.Lock()
	defer tr.lk.Unlock()

	tr.timedOutKs = append(tr.timedOutKs, tks...)
}

func (tr *timeoutRecorder) timedOutCount() int {
	tr.lk.Lock()
	defer tr.lk.Unlock()

	return len(tr.timedOutKs)
}

func (tr *timeoutRecorder) clear() {
	tr.lk.Lock()
	defer tr.lk.Unlock()

	tr.timedOutKs = nil
}

func TestDontHaveTimeoutMgrTimeout(t *testing.T) {
	firstks := testutil.GenerateCids(2)
	secondks := append(firstks, testutil.GenerateCids(3)...)
	latency := time.Millisecond * 20
	latMultiplier := 2
	expProcessTime := 5 * time.Millisecond
	expectedTimeout := expProcessTime + latency*time.Duration(latMultiplier)
	clock := clock.NewMock()
	pinged := make(chan struct{})
	pc := &mockPeerConn{latency: latency, clock: clock, pinged: pinged}
	tr := timeoutRecorder{}
	timeoutsTriggered := make(chan struct{})
	dhtm := newDontHaveTimeoutMgrWithParams(pc, tr.onTimeout,
		dontHaveTimeout, maxTimeout, latMultiplier, messageLatencyMultiplier, expProcessTime, clock, timeoutsTriggered)
	dhtm.Start()
	defer dhtm.Shutdown()
	<-pinged
	// Add first set of keys
	dhtm.AddPending(firstks)

	// Wait for less than the expected timeout
	clock.Add(expectedTimeout - 10*time.Millisecond)

	// At this stage no keys should have timed out
	if tr.timedOutCount() > 0 {
		t.Fatal("expected timeout not to have happened yet")
	}

	// Add second set of keys
	dhtm.AddPending(secondks)

	// Wait until after the expected timeout
	clock.Add(20 * time.Millisecond)

	<-timeoutsTriggered

	// At this stage first set of keys should have timed out
	if tr.timedOutCount() != len(firstks) {
		t.Fatal("expected timeout", tr.timedOutCount(), len(firstks))
	}
	// Clear the recorded timed out keys
	tr.clear()

	// Sleep until the second set of keys should have timed out
	clock.Add(expectedTimeout + 10*time.Millisecond)

	<-timeoutsTriggered

	// At this stage all keys should have timed out. The second set included
	// the first set of keys, but they were added before the first set timed
	// out, so only the remaining keys should have beed added.
	if tr.timedOutCount() != len(secondks)-len(firstks) {
		t.Fatal("expected second set of keys to timeout")
	}
}

func TestDontHaveTimeoutMgrCancel(t *testing.T) {
	ks := testutil.GenerateCids(3)
	latency := time.Millisecond * 10
	latMultiplier := 1
	expProcessTime := time.Duration(0)
	expectedTimeout := latency
	clock := clock.NewMock()
	pinged := make(chan struct{})
	pc := &mockPeerConn{latency: latency, clock: clock, pinged: pinged}
	tr := timeoutRecorder{}
	timeoutsTriggered := make(chan struct{})
	dhtm := newDontHaveTimeoutMgrWithParams(pc, tr.onTimeout,
		dontHaveTimeout, maxTimeout, latMultiplier, messageLatencyMultiplier, expProcessTime, clock, timeoutsTriggered)
	dhtm.Start()
	defer dhtm.Shutdown()
	<-pinged

	// Add keys
	dhtm.AddPending(ks)
	clock.Add(5 * time.Millisecond)

	// Cancel keys
	cancelCount := 1
	dhtm.CancelPending(ks[:cancelCount])

	// Wait for the expected timeout
	clock.Add(expectedTimeout)

	<-timeoutsTriggered

	// At this stage all non-cancelled keys should have timed out
	if tr.timedOutCount() != len(ks)-cancelCount {
		t.Fatal("expected timeout")
	}
}

func TestDontHaveTimeoutWantCancelWant(t *testing.T) {
	ks := testutil.GenerateCids(3)
	latency := time.Millisecond * 20
	latMultiplier := 1
	expProcessTime := time.Duration(0)
	expectedTimeout := latency
	clock := clock.NewMock()
	pinged := make(chan struct{})
	pc := &mockPeerConn{latency: latency, clock: clock, pinged: pinged}
	tr := timeoutRecorder{}
	timeoutsTriggered := make(chan struct{})

	dhtm := newDontHaveTimeoutMgrWithParams(pc, tr.onTimeout,
		dontHaveTimeout, maxTimeout, latMultiplier, messageLatencyMultiplier, expProcessTime, clock, timeoutsTriggered)
	dhtm.Start()
	defer dhtm.Shutdown()
	<-pinged

	// Add keys
	dhtm.AddPending(ks)

	// Wait for a short time
	clock.Add(expectedTimeout - 10*time.Millisecond)

	// Cancel two keys
	dhtm.CancelPending(ks[:2])

	clock.Add(5 * time.Millisecond)

	// Add back one cancelled key
	dhtm.AddPending(ks[:1])

	// Wait till after initial timeout
	clock.Add(10 * time.Millisecond)

	<-timeoutsTriggered

	// At this stage only the key that was never cancelled should have timed out
	if tr.timedOutCount() != 1 {
		t.Fatal("expected one key to timeout")
	}

	// Wait till after added back key should time out
	clock.Add(latency)

	<-timeoutsTriggered

	// At this stage the key that was added back should also have timed out
	if tr.timedOutCount() != 2 {
		t.Fatal("expected added back key to timeout")
	}
}

func TestDontHaveTimeoutRepeatedAddPending(t *testing.T) {
	ks := testutil.GenerateCids(10)
	latency := time.Millisecond * 5
	latMultiplier := 1
	expProcessTime := time.Duration(0)
	clock := clock.NewMock()
	pinged := make(chan struct{})
	pc := &mockPeerConn{latency: latency, clock: clock, pinged: pinged}
	tr := timeoutRecorder{}
	timeoutsTriggered := make(chan struct{})

	dhtm := newDontHaveTimeoutMgrWithParams(pc, tr.onTimeout,
		dontHaveTimeout, maxTimeout, latMultiplier, messageLatencyMultiplier, expProcessTime, clock, timeoutsTriggered)
	dhtm.Start()
	defer dhtm.Shutdown()
	<-pinged

	// Add keys repeatedly
	for _, c := range ks {
		dhtm.AddPending([]cid.Cid{c})
	}

	// Wait for the expected timeout
	clock.Add(latency + 5*time.Millisecond)

	<-timeoutsTriggered

	// At this stage all keys should have timed out
	if tr.timedOutCount() != len(ks) {
		t.Fatal("expected timeout")
	}
}

func TestDontHaveTimeoutMgrMessageLatency(t *testing.T) {
	ks := testutil.GenerateCids(2)
	latency := time.Millisecond * 40
	latMultiplier := 1
	expProcessTime := time.Duration(0)
	msgLatencyMultiplier := 1
	clock := clock.NewMock()
	pinged := make(chan struct{})
	pc := &mockPeerConn{latency: latency, clock: clock, pinged: pinged}
	tr := timeoutRecorder{}
	timeoutsTriggered := make(chan struct{})

	dhtm := newDontHaveTimeoutMgrWithParams(pc, tr.onTimeout,
		dontHaveTimeout, maxTimeout, latMultiplier, msgLatencyMultiplier, expProcessTime, clock, timeoutsTriggered)
	dhtm.Start()
	defer dhtm.Shutdown()
	<-pinged
	// Add keys
	dhtm.AddPending(ks)

	// expectedTimeout
	// = expProcessTime + latency*time.Duration(latMultiplier)
	// = 0 + 40ms * 1
	// = 40ms

	// Wait for less than the expected timeout
	clock.Add(25 * time.Millisecond)

	// Receive two message latency updates
	dhtm.UpdateMessageLatency(time.Millisecond * 20)
	dhtm.UpdateMessageLatency(time.Millisecond * 10)

	// alpha is 0.5 so timeout should be
	// = (20ms * alpha) + (10ms * (1 - alpha))
	// = (20ms * 0.5) + (10ms * 0.5)
	// = 15ms
	// We've already slept for 25ms so with the new 15ms timeout
	// the keys should have timed out

	// Give the queue some time to process the updates
	clock.Add(5 * time.Millisecond)

	<-timeoutsTriggered

	if tr.timedOutCount() != len(ks) {
		t.Fatal("expected keys to timeout")
	}
}

func TestDontHaveTimeoutMgrMessageLatencyMax(t *testing.T) {
	ks := testutil.GenerateCids(2)
	clock := clock.NewMock()
	pinged := make(chan struct{})
	pc := &mockPeerConn{latency: time.Second, clock: clock, pinged: pinged}
	tr := timeoutRecorder{}
	msgLatencyMultiplier := 1
	testMaxTimeout := time.Millisecond * 10
	timeoutsTriggered := make(chan struct{})

	dhtm := newDontHaveTimeoutMgrWithParams(pc, tr.onTimeout,
		dontHaveTimeout, testMaxTimeout, pingLatencyMultiplier, msgLatencyMultiplier, maxExpectedWantProcessTime, clock, timeoutsTriggered)
	dhtm.Start()
	defer dhtm.Shutdown()
	<-pinged
	// Add keys
	dhtm.AddPending(ks)

	// Receive a message latency update that would make the timeout greater
	// than the maximum timeout
	dhtm.UpdateMessageLatency(testMaxTimeout * 4)

	// Sleep until just after the maximum timeout
	clock.Add(testMaxTimeout + 5*time.Millisecond)

	<-timeoutsTriggered

	// Keys should have timed out
	if tr.timedOutCount() != len(ks) {
		t.Fatal("expected keys to timeout")
	}
}

func TestDontHaveTimeoutMgrUsesDefaultTimeoutIfPingError(t *testing.T) {
	ks := testutil.GenerateCids(2)
	latency := time.Millisecond * 1
	latMultiplier := 2
	expProcessTime := 2 * time.Millisecond
	defaultTimeout := 10 * time.Millisecond
	expectedTimeout := expProcessTime + defaultTimeout
	tr := timeoutRecorder{}
	clock := clock.NewMock()
	pinged := make(chan struct{})
	pc := &mockPeerConn{latency: latency, clock: clock, pinged: pinged, err: fmt.Errorf("ping error")}
	timeoutsTriggered := make(chan struct{})

	dhtm := newDontHaveTimeoutMgrWithParams(pc, tr.onTimeout,
		defaultTimeout, dontHaveTimeout, latMultiplier, messageLatencyMultiplier, expProcessTime, clock, timeoutsTriggered)
	dhtm.Start()
	defer dhtm.Shutdown()
	<-pinged

	// Add keys
	dhtm.AddPending(ks)

	// Sleep for less than the expected timeout
	clock.Add(expectedTimeout - 5*time.Millisecond)

	// At this stage no timeout should have happened yet
	if tr.timedOutCount() > 0 {
		t.Fatal("expected timeout not to have happened yet")
	}

	// Sleep until after the expected timeout
	clock.Add(10 * time.Millisecond)

	<-timeoutsTriggered

	// Now the keys should have timed out
	if tr.timedOutCount() != len(ks) {
		t.Fatal("expected timeout")
	}
}

func TestDontHaveTimeoutMgrUsesDefaultTimeoutIfLatencyLonger(t *testing.T) {
	ks := testutil.GenerateCids(2)
	latency := time.Millisecond * 20
	latMultiplier := 1
	expProcessTime := time.Duration(0)
	defaultTimeout := 10 * time.Millisecond
	clock := clock.NewMock()
	pinged := make(chan struct{})
	pc := &mockPeerConn{latency: latency, clock: clock, pinged: pinged}
	tr := timeoutRecorder{}
	timeoutsTriggered := make(chan struct{})

	dhtm := newDontHaveTimeoutMgrWithParams(pc, tr.onTimeout,
		defaultTimeout, dontHaveTimeout, latMultiplier, messageLatencyMultiplier, expProcessTime, clock, timeoutsTriggered)
	dhtm.Start()
	defer dhtm.Shutdown()
	<-pinged

	// Add keys
	dhtm.AddPending(ks)

	// Sleep for less than the default timeout
	clock.Add(defaultTimeout - 5*time.Millisecond)

	// At this stage no timeout should have happened yet
	if tr.timedOutCount() > 0 {
		t.Fatal("expected timeout not to have happened yet")
	}

	// Sleep until after the default timeout
	clock.Add(defaultTimeout * 2)

	<-timeoutsTriggered

	// Now the keys should have timed out
	if tr.timedOutCount() != len(ks) {
		t.Fatal("expected timeout")
	}
}

func TestDontHaveTimeoutNoTimeoutAfterShutdown(t *testing.T) {
	ks := testutil.GenerateCids(2)
	latency := time.Millisecond * 10
	latMultiplier := 1
	expProcessTime := time.Duration(0)
	clock := clock.NewMock()
	pinged := make(chan struct{})
	pc := &mockPeerConn{latency: latency, clock: clock, pinged: pinged}
	tr := timeoutRecorder{}
	timeoutsTriggered := make(chan struct{})

	dhtm := newDontHaveTimeoutMgrWithParams(pc, tr.onTimeout,
		dontHaveTimeout, maxTimeout, latMultiplier, messageLatencyMultiplier, expProcessTime, clock, timeoutsTriggered)
	dhtm.Start()
	defer dhtm.Shutdown()
	<-pinged

	// Add keys
	dhtm.AddPending(ks)

	// Wait less than the timeout
	clock.Add(latency - 5*time.Millisecond)

	// Shutdown the manager
	dhtm.Shutdown()

	// Wait for the expected timeout
	clock.Add(10 * time.Millisecond)

	// Manager was shut down so timeout should not have fired
	if tr.timedOutCount() != 0 {
		t.Fatal("expected no timeout after shutdown")
	}
}
