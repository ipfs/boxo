package decision

import (
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-peertaskqueue/peertracker"
	"github.com/libp2p/go-libp2p/core/peer"
)

// benchSink prevents the compiler from hoisting or eliminating comparator
// calls whose result would otherwise go unused.
var benchSink atomic.Bool

// realisticPeerID returns a 38-byte peer.ID whose bytes look like an
// identity-multihash of an ed25519 pubkey. The short test IDs used elsewhere
// understate hash cost at layer 5; these do not.
func realisticPeerID(tb testing.TB) peer.ID {
	b := make([]byte, 38)
	if _, err := rand.Read(b); err != nil {
		tb.Fatal(err)
	}
	b[0], b[1] = 0x00, 0x24
	return peer.ID(b)
}

// BenchmarkComparatorVsUpstream measures per-call cost of the fair
// comparator against peertracker.DefaultPeerComparator on realistic peer
// IDs. Any added chokepoint would show up here as a large gap.
func BenchmarkComparatorVsUpstream(b *testing.B) {
	idA := realisticPeerID(b)
	idB := realisticPeerID(b)
	a := newTestTracker(string(idA), 50)
	c := newTestTracker(string(idB), 50)

	b.Run("upstream_default", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchSink.Store(peertracker.DefaultPeerComparator(a, c))
		}
	})

	b.Run("fair_layer2_shortcut", func(b *testing.B) {
		s := newPeerScheduler()
		s.markServed(idB) // idA is never-served, layer 2 fires
		cmp := fairPeerComparator(s)
		for i := 0; i < b.N; i++ {
			benchSink.Store(cmp(a, c))
		}
	})

	b.Run("fair_layer5_fallthrough", func(b *testing.B) {
		s := newPeerScheduler()
		s.markServed(idA)
		s.markServed(idB)
		cmp := fairPeerComparator(s)
		for i := 0; i < b.N; i++ {
			benchSink.Store(cmp(a, c))
		}
	})
}

// BenchmarkMarkServed measures the cost of the write-lock path. The engine
// calls this once per emitted envelope, so at 400k envelopes/sec it must
// stay well under 1us.
func BenchmarkMarkServed(b *testing.B) {
	s := newPeerScheduler()
	id := realisticPeerID(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.markServed(id)
	}
}

// BenchmarkContended exercises the realistic shape of load: many readers
// (the heap is fixing itself up on every push/pop) and one writer (the
// single task-worker goroutine calling markServed on each envelope). A
// chokepoint in the RWMutex would show up as reader throughput collapsing
// when the writer is active.
func BenchmarkContended(b *testing.B) {
	const readers = 16

	for _, writeHz := range []int{0, 10_000, 100_000, 400_000} {
		name := fmt.Sprintf("readers=%d_writes/s=%d", readers, writeHz)
		b.Run(name, func(b *testing.B) {
			s := newPeerScheduler()

			// Pre-populate so isStarved reads actually find entries.
			ids := make([]peer.ID, 64)
			for i := range ids {
				ids[i] = realisticPeerID(b)
				s.markServed(ids[i])
			}

			trackers := make([]*peertracker.PeerTracker, len(ids))
			for i, id := range ids {
				trackers[i] = newTestTracker(string(id), 50)
			}
			cmp := fairPeerComparator(s)

			stop := make(chan struct{})
			var wg sync.WaitGroup

			// Writer goroutine: calls markServed at the target rate.
			if writeHz > 0 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					interval := time.Second / time.Duration(writeHz)
					t := time.NewTicker(interval)
					defer t.Stop()
					i := 0
					for {
						select {
						case <-stop:
							return
						case <-t.C:
							s.markServed(ids[i%len(ids)])
							i++
						}
					}
				}()
			}

			var ops atomic.Uint64
			b.ResetTimer()
			b.SetParallelism(readers)
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					pa := trackers[i%len(trackers)]
					pc := trackers[(i+1)%len(trackers)]
					benchSink.Store(cmp(pa, pc))
					i++
					ops.Add(1)
				}
			})
			b.StopTimer()

			close(stop)
			wg.Wait()
			b.ReportMetric(float64(ops.Load())/b.Elapsed().Seconds(), "cmp/s")
		})
	}
}
