package messagequeue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/internal/testutil"
	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

type mockPeerConn struct {
	err       error
	latency   time.Duration
	latencies []time.Duration
}

func (pc *mockPeerConn) Ping(ctx context.Context) ping.Result {
	timer := time.NewTimer(pc.latency)
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

func TestDontHaveTimeoutMgrTimeout(t *testing.T) {
	firstks := testutil.GenerateCids(2)
	secondks := append(firstks, testutil.GenerateCids(3)...)
	latency := time.Millisecond * 10
	latMultiplier := 2
	expProcessTime := 5 * time.Millisecond
	expectedTimeout := expProcessTime + latency*time.Duration(latMultiplier)
	ctx := context.Background()
	pc := &mockPeerConn{latency: latency}
	tr := timeoutRecorder{}

	dhtm := newDontHaveTimeoutMgrWithParams(ctx, pc, tr.onTimeout,
		dontHaveTimeout, latMultiplier, expProcessTime)
	dhtm.Start()

	// Add first set of keys
	dhtm.AddPending(firstks)

	// Wait for less than the expected timeout
	time.Sleep(expectedTimeout - 5*time.Millisecond)

	// At this stage no keys should have timed out
	if len(tr.timedOutKs) > 0 {
		t.Fatal("expected timeout not to have happened yet")
	}

	// Add second set of keys
	dhtm.AddPending(secondks)

	// Wait until after the expected timeout
	time.Sleep(10 * time.Millisecond)

	// At this stage first set of keys should have timed out
	if len(tr.timedOutKs) != len(firstks) {
		t.Fatal("expected timeout")
	}

	// Clear the recorded timed out keys
	tr.timedOutKs = nil

	// Sleep until the second set of keys should have timed out
	time.Sleep(expectedTimeout)

	// At this stage all keys should have timed out. The second set included
	// the first set of keys, but they were added before the first set timed
	// out, so only the remaining keys should have beed added.
	if len(tr.timedOutKs) != len(secondks)-len(firstks) {
		t.Fatal("expected second set of keys to timeout")
	}
}

func TestDontHaveTimeoutMgrCancel(t *testing.T) {
	ks := testutil.GenerateCids(3)
	latency := time.Millisecond * 10
	latMultiplier := 1
	expProcessTime := time.Duration(0)
	expectedTimeout := latency
	ctx := context.Background()
	pc := &mockPeerConn{latency: latency}
	tr := timeoutRecorder{}

	dhtm := newDontHaveTimeoutMgrWithParams(ctx, pc, tr.onTimeout,
		dontHaveTimeout, latMultiplier, expProcessTime)
	dhtm.Start()

	// Add keys
	dhtm.AddPending(ks)
	time.Sleep(5 * time.Millisecond)

	// Cancel keys
	cancelCount := 1
	dhtm.CancelPending(ks[:cancelCount])

	// Wait for the expected timeout
	time.Sleep(expectedTimeout)

	// At this stage all non-cancelled keys should have timed out
	if len(tr.timedOutKs) != len(ks)-cancelCount {
		t.Fatal("expected timeout")
	}
}

func TestDontHaveTimeoutWantCancelWant(t *testing.T) {
	ks := testutil.GenerateCids(3)
	latency := time.Millisecond * 20
	latMultiplier := 1
	expProcessTime := time.Duration(0)
	expectedTimeout := latency
	ctx := context.Background()
	pc := &mockPeerConn{latency: latency}
	tr := timeoutRecorder{}

	dhtm := newDontHaveTimeoutMgrWithParams(ctx, pc, tr.onTimeout,
		dontHaveTimeout, latMultiplier, expProcessTime)
	dhtm.Start()

	// Add keys
	dhtm.AddPending(ks)

	// Wait for a short time
	time.Sleep(expectedTimeout - 10*time.Millisecond)

	// Cancel two keys
	dhtm.CancelPending(ks[:2])

	time.Sleep(5 * time.Millisecond)

	// Add back one cancelled key
	dhtm.AddPending(ks[:1])

	// Wait till after initial timeout
	time.Sleep(10 * time.Millisecond)

	// At this stage only the key that was never cancelled should have timed out
	if len(tr.timedOutKs) != 1 {
		t.Fatal("expected one key to timeout")
	}

	// Wait till after added back key should time out
	time.Sleep(latency)

	// At this stage the key that was added back should also have timed out
	if len(tr.timedOutKs) != 2 {
		t.Fatal("expected added back key to timeout")
	}
}

func TestDontHaveTimeoutRepeatedAddPending(t *testing.T) {
	ks := testutil.GenerateCids(10)
	latency := time.Millisecond * 5
	latMultiplier := 1
	expProcessTime := time.Duration(0)
	ctx := context.Background()
	pc := &mockPeerConn{latency: latency}
	tr := timeoutRecorder{}

	dhtm := newDontHaveTimeoutMgrWithParams(ctx, pc, tr.onTimeout,
		dontHaveTimeout, latMultiplier, expProcessTime)
	dhtm.Start()

	// Add keys repeatedly
	for _, c := range ks {
		dhtm.AddPending([]cid.Cid{c})
	}

	// Wait for the expected timeout
	time.Sleep(latency + 5*time.Millisecond)

	// At this stage all keys should have timed out
	if len(tr.timedOutKs) != len(ks) {
		t.Fatal("expected timeout")
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
	ctx := context.Background()
	pc := &mockPeerConn{latency: latency, err: fmt.Errorf("ping error")}

	dhtm := newDontHaveTimeoutMgrWithParams(ctx, pc, tr.onTimeout,
		defaultTimeout, latMultiplier, expProcessTime)
	dhtm.Start()

	// Add keys
	dhtm.AddPending(ks)

	// Sleep for less than the expected timeout
	time.Sleep(expectedTimeout - 5*time.Millisecond)

	// At this stage no timeout should have happened yet
	if len(tr.timedOutKs) > 0 {
		t.Fatal("expected timeout not to have happened yet")
	}

	// Sleep until after the expected timeout
	time.Sleep(10 * time.Millisecond)

	// Now the keys should have timed out
	if len(tr.timedOutKs) != len(ks) {
		t.Fatal("expected timeout")
	}
}

func TestDontHaveTimeoutMgrUsesDefaultTimeoutIfLatencyLonger(t *testing.T) {
	ks := testutil.GenerateCids(2)
	latency := time.Millisecond * 20
	latMultiplier := 1
	expProcessTime := time.Duration(0)
	defaultTimeout := 10 * time.Millisecond
	tr := timeoutRecorder{}
	ctx := context.Background()
	pc := &mockPeerConn{latency: latency}

	dhtm := newDontHaveTimeoutMgrWithParams(ctx, pc, tr.onTimeout,
		defaultTimeout, latMultiplier, expProcessTime)
	dhtm.Start()

	// Add keys
	dhtm.AddPending(ks)

	// Sleep for less than the default timeout
	time.Sleep(defaultTimeout - 5*time.Millisecond)

	// At this stage no timeout should have happened yet
	if len(tr.timedOutKs) > 0 {
		t.Fatal("expected timeout not to have happened yet")
	}

	// Sleep until after the default timeout
	time.Sleep(10 * time.Millisecond)

	// Now the keys should have timed out
	if len(tr.timedOutKs) != len(ks) {
		t.Fatal("expected timeout")
	}
}

func TestDontHaveTimeoutNoTimeoutAfterShutdown(t *testing.T) {
	ks := testutil.GenerateCids(2)
	latency := time.Millisecond * 10
	latMultiplier := 1
	expProcessTime := time.Duration(0)
	ctx := context.Background()
	pc := &mockPeerConn{latency: latency}

	var lk sync.Mutex
	var timedOutKs []cid.Cid
	onTimeout := func(tks []cid.Cid) {
		lk.Lock()
		defer lk.Unlock()
		timedOutKs = append(timedOutKs, tks...)
	}
	dhtm := newDontHaveTimeoutMgrWithParams(ctx, pc, onTimeout,
		dontHaveTimeout, latMultiplier, expProcessTime)
	dhtm.Start()

	// Add keys
	dhtm.AddPending(ks)

	// Wait less than the timeout
	time.Sleep(latency - 5*time.Millisecond)

	// Shutdown the manager
	dhtm.Shutdown()

	// Wait for the expected timeout
	time.Sleep(10 * time.Millisecond)

	// Manager was shut down so timeout should not have fired
	if len(timedOutKs) != 0 {
		t.Fatal("expected no timeout after shutdown")
	}
}
