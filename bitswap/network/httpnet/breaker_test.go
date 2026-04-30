package httpnet

import (
	"sync"
	"testing"
)

func TestBreaker_CounterIsSharedPerKey(t *testing.T) {
	b := newBreaker()
	a1 := b.counter("k")
	a2 := b.counter("k")
	if a1 != a2 {
		t.Fatalf("counter(k) returned different pointers across calls (%p vs %p)", a1, a2)
	}
	a1.Add(3)
	if got := a2.Load(); got != 3 {
		t.Fatalf("a2 sees %d, want 3 (counters must alias)", got)
	}
}

func TestBreaker_DistinctKeysAreIndependent(t *testing.T) {
	b := newBreaker()
	x := b.counter("x")
	y := b.counter("y")
	x.Add(7)
	if got := y.Load(); got != 0 {
		t.Fatalf("y leaked from x: y=%d, want 0", got)
	}
}

func TestBreaker_ResetDropsCounter(t *testing.T) {
	b := newBreaker()
	c1 := b.counter("k")
	c1.Add(5)
	b.reset("k")

	// After reset, a fresh counter is allocated; it must not carry the
	// old value, and it must be a different pointer than the old one.
	c2 := b.counter("k")
	if c2 == c1 {
		t.Fatalf("reset returned the same pointer; want a fresh counter")
	}
	if got := c2.Load(); got != 0 {
		t.Fatalf("fresh counter has value %d, want 0", got)
	}
	// The old pointer is still usable but orphaned: incrementing it
	// must not affect the new counter.
	c1.Add(1)
	if got := c2.Load(); got != 0 {
		t.Fatalf("new counter affected by orphaned pointer: %d, want 0", got)
	}
}

func TestBreaker_ConcurrentCounterAndReset(t *testing.T) {
	// Smoke test: counter() and reset() running concurrently must not
	// deadlock or race-detect.
	b := newBreaker()

	var wg sync.WaitGroup
	const N = 20
	for range N {
		wg.Go(func() {
			for range 100 {
				b.counter("hot").Add(1)
			}
		})
		wg.Go(func() {
			for range 100 {
				b.reset("hot")
			}
		})
	}
	wg.Wait()
}
