package decision

import (
	"context"
	"fmt"
	"testing"
	"time"

	blockstore "github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-peertaskqueue/peertask"
	"github.com/ipfs/go-peertaskqueue/peertracker"
	"github.com/libp2p/go-libp2p/core/peer"
)

// newTestTracker builds a PeerTracker with the given pending-task count, so
// tests can drive fairPeerComparator without starting an engine.
func newTestTracker(id string, pending int) *peertracker.PeerTracker {
	pt := peertracker.New(peer.ID(id), &peertracker.DefaultTaskMerger{}, 0)
	if pending == 0 {
		return pt
	}
	tasks := make([]peertask.Task, pending)
	for i := range tasks {
		tasks[i] = peertask.Task{
			Topic:    fmt.Sprintf("%s-%d", id, i),
			Priority: 1,
			Work:     1,
		}
	}
	pt.PushTasks(tasks...)
	return pt
}

// TestFairSchedulerLowPendingPeerIsServed reproduces the starvation
// scenario from https://github.com/ipfs/boxo/issues/1141. Before the fair
// comparator, the engine served heavy peers in a loop and a peer with a
// single pending want waited indefinitely.
func TestFairSchedulerLowPendingPeerIsServed(t *testing.T) {
	const (
		heavyPeerCount     = 10
		heavyWantCount     = 50
		maxEnvelopesToWait = heavyPeerCount + 2
	)

	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	heavyCids := make([]string, heavyWantCount)
	for i := range heavyCids {
		heavyCids[i] = fmt.Sprintf("heavy-%03d", i)
		if err := bs.Put(context.Background(), blocks.NewBlock([]byte(heavyCids[i]))); err != nil {
			t.Fatal(err)
		}
	}
	newPeerCid := "new-peer-block"
	if err := bs.Put(context.Background(), blocks.NewBlock([]byte(newPeerCid))); err != nil {
		t.Fatal(err)
	}

	e := newEngineForTesting(bs, &fakePeerTagger{}, "localhost", 0,
		WithScoreLedger(NewTestScoreLedger(shortTerm, nil)),
		WithBlockstoreWorkerCount(4))
	defer e.Close()

	heavyPeers := make([]peer.ID, heavyPeerCount)
	for i := range heavyPeers {
		heavyPeers[i] = peer.ID(fmt.Sprintf("heavy-peer-%02d", i))
		partnerWantBlocks(e, heavyCids, heavyPeers[i])
	}

	newPeer := peer.ID("new-peer")
	partnerWantBlocks(e, []string{newPeerCid}, newPeer)

	newPeerBlockCid := blocks.NewBlock([]byte(newPeerCid)).Cid()

	for i := 0; i < maxEnvelopesToWait; i++ {
		select {
		case next := <-e.Outbox():
			env := <-next
			if env == nil {
				continue
			}
			for _, blk := range env.Message.Blocks() {
				if blk.Cid() == newPeerBlockCid {
					env.Sent()
					return
				}
			}
			env.Sent()
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for envelope %d", i)
		}
	}
	t.Fatalf("new peer was not served within %d envelopes (starvation regression)", maxEnvelopesToWait)
}

// TestFairSchedulerForgetsDisconnectedPeer verifies forget drops the
// per-peer entry, so the scheduler does not grow without bound.
func TestFairSchedulerForgetsDisconnectedPeer(t *testing.T) {
	s := newPeerScheduler()
	p := peer.ID("p")

	s.markServed(p)
	if !s.isStarved(p, time.Now().Add(peerStarvationTimeout+time.Second)) {
		t.Fatal("peer should be starved past timeout")
	}
	s.forget(p)
	if _, present := s.lastServedAt.Load(p); present {
		t.Fatal("scheduler still holds state for forgotten peer")
	}
}

// TestFairComparatorEmptyQueueLoses covers layer 1: a peer with nothing
// pending cannot outrank a peer with queued work.
func TestFairComparatorEmptyQueueLoses(t *testing.T) {
	cmp := fairPeerComparator(newPeerScheduler())
	busy := newTestTracker("busy", 5)
	empty := newTestTracker("empty", 0)

	if cmp(empty, busy) {
		t.Fatal("empty-pending peer must not outrank pending peer")
	}
	if !cmp(busy, empty) {
		t.Fatal("pending peer must outrank empty-pending peer")
	}
}

// TestFairComparatorNeverServedBeatsSteady covers layer 2 (never-served
// branch): a fresh peer with one pending want outranks a just-served peer,
// even when the steady peer holds vastly more pending work. This is #1141
// at the comparator level.
func TestFairComparatorNeverServedBeatsSteady(t *testing.T) {
	s := newPeerScheduler()
	s.markServed(peer.ID("steady"))
	cmp := fairPeerComparator(s)

	fresh := newTestTracker("fresh", 1)
	steady := newTestTracker("steady", 100)

	if !cmp(fresh, steady) {
		t.Fatal("never-served peer must outrank steady peer")
	}
	if cmp(steady, fresh) {
		t.Fatal("steady peer must not outrank never-served peer")
	}
}

// TestFairComparatorTimeoutOverridesSteady covers layer 2 (timeout
// branch): a peer last served longer than peerStarvationTimeout ago
// outranks a just-served peer.
func TestFairComparatorTimeoutOverridesSteady(t *testing.T) {
	s := newPeerScheduler()
	s.recordServedAt(peer.ID("late"), time.Now().Add(-peerStarvationTimeout-time.Second))
	s.recordServedAt(peer.ID("steady"), time.Now())
	cmp := fairPeerComparator(s)

	late := newTestTracker("late", 1)
	steady := newTestTracker("steady", 100)

	if !cmp(late, steady) {
		t.Fatal("peer waiting past timeout must outrank steady peer")
	}
}

// TestFairComparatorCappedPendingAndTiebreak covers layers 4 and 5: higher
// capped pending wins, and ties on capped pending fall through to the
// salted-hash tiebreak.
func TestFairComparatorCappedPendingAndTiebreak(t *testing.T) {
	s := newPeerScheduler()
	now := time.Now()
	s.recordServedAt(peer.ID("a"), now)
	s.recordServedAt(peer.ID("b"), now)
	cmp := fairPeerComparator(s)

	// 17 caps to 16, 3 stays at 3: higher capped pending wins.
	a := newTestTracker("a", 17)
	b := newTestTracker("b", 3)
	if !cmp(a, b) {
		t.Fatal("peer with higher capped pending must win (17 > 3 after cap)")
	}

	// 1000 and 17 both cap to 16, so layer 4 ties. Layer 5 must break
	// antisymmetrically (exactly one side wins) and stably (same winner on
	// every call) or the heap breaks. The direction depends on the
	// per-process seed.
	aBig := newTestTracker("a", 1000)
	bMed := newTestTracker("b", 17)
	first := cmp(aBig, bMed)
	if first == cmp(bMed, aBig) {
		t.Fatal("salted tiebreak must be antisymmetric")
	}
	for i := 0; i < 100; i++ {
		if cmp(aBig, bMed) != first {
			t.Fatal("salted tiebreak must be stable within a scheduler instance")
		}
	}
}

// TestFairComparatorTiebreakSaltVariesAcrossSchedulers checks that the
// layer-5 tiebreak resists forgery. Two independent schedulers disagree on
// the winner for roughly half of all peer.ID pairs; a deterministic
// byte-compare tiebreak would agree on every pair.
func TestFairComparatorTiebreakSaltVariesAcrossSchedulers(t *testing.T) {
	const pairs = 256
	now := time.Now()

	flips := 0
	for i := 0; i < pairs; i++ {
		s1 := newPeerScheduler()
		s2 := newPeerScheduler()
		idA := peer.ID(fmt.Sprintf("peer-a-%03d", i))
		idB := peer.ID(fmt.Sprintf("peer-b-%03d", i))

		s1.recordServedAt(idA, now)
		s1.recordServedAt(idB, now)
		s2.recordServedAt(idA, now)
		s2.recordServedAt(idB, now)

		a := newTestTracker(string(idA), 100)
		b := newTestTracker(string(idB), 100)
		if fairPeerComparator(s1)(a, b) != fairPeerComparator(s2)(a, b) {
			flips++
		}
	}
	// Expected around 50%. The band stays wide so the test does not flake; a
	// deterministic tiebreak produces 0 flips and fails loudly.
	if flips < pairs/4 || flips > pairs*3/4 {
		t.Fatalf("tiebreak looks non-random across schedulers: %d/%d flips", flips, pairs)
	}
}

// BenchmarkFairPeerComparator measures per-call comparator cost. The
// engine runs this on every heap fix-up, so the layer-2 shortcut and the
// full layer-5 fall-through both matter under load.
func BenchmarkFairPeerComparator(b *testing.B) {
	s := newPeerScheduler()
	s.markServed(peer.ID("steady"))
	cmp := fairPeerComparator(s)

	steady := newTestTracker("steady", 50)
	fresh := newTestTracker("fresh", 1)

	b.Run("layer2_starved_shortcut", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = cmp(fresh, steady)
		}
	})

	// Two steady peers, neither starved, so the comparator walks every
	// layer down to the peer.ID tiebreak.
	s2 := newPeerScheduler()
	s2.markServed(peer.ID("a"))
	s2.markServed(peer.ID("b"))
	cmp2 := fairPeerComparator(s2)
	aT := newTestTracker("a", 100)
	bT := newTestTracker("b", 100)

	b.Run("layer5_fall_through", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = cmp2(aT, bT)
		}
	})
}
