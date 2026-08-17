package httpnet

import (
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestConnectionRegistry(t *testing.T) {
	pngr := newPinger(nil)
	p := peer.ID("test-peer")

	if pngr.isConnected(p) {
		t.Fatal("new registry should be empty")
	}

	pngr.markConnected(p)
	pngr.markConnected(p) // idempotent
	if !pngr.isConnected(p) {
		t.Fatal("peer should be connected")
	}

	pngr.recordLatency(p, time.Second)

	pngr.markDisconnected(p)
	if pngr.isConnected(p) {
		t.Error("peer should be disconnected")
	}
	if pngr.latency(p) != 0 {
		t.Error("latency should be wiped on disconnect")
	}
}

func TestRecordLatencyEWMA(t *testing.T) {
	pngr := newPinger(nil)
	p := peer.ID("test-peer")

	pngr.recordLatency(p, time.Second)
	if got := pngr.latency(p); got != time.Second {
		t.Fatalf("first sample should be taken as-is: %s", got)
	}

	pngr.recordLatency(p, 2*time.Second)
	want := time.Duration(0.9*float64(time.Second) + 0.1*float64(2*time.Second))
	if got := pngr.latency(p); got != want {
		t.Errorf("EWMA blend: want %s, got %s", want, got)
	}
}

func TestRecordLatencyIfConnected(t *testing.T) {
	pngr := newPinger(nil)
	p := peer.ID("test-peer")

	pngr.recordLatencyIfConnected(p, time.Second)
	if pngr.latency(p) != 0 {
		t.Fatal("sample for a non-connected peer should be dropped")
	}

	pngr.markConnected(p)
	pngr.recordLatencyIfConnected(p, time.Second)
	if pngr.latency(p) != time.Second {
		t.Fatal("sample for a connected peer should be recorded")
	}

	// A zero measurement from a coarse clock is floored, not dropped: a
	// measured peer must never look unmeasured.
	other := peer.ID("zero-sample-peer")
	pngr.markConnected(other)
	pngr.recordLatencyIfConnected(other, 0)
	if pngr.latency(other) <= 0 {
		t.Fatal("zero sample should be floored to a positive latency")
	}
}

func TestPingerConcurrentHammer(t *testing.T) {
	pngr := newPinger(nil)
	peers := []peer.ID{"a", "b", "c"}

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range 500 {
				p := peers[(i+j)%len(peers)]
				switch j % 4 {
				case 0:
					pngr.markConnected(p)
				case 1:
					pngr.recordLatencyIfConnected(p, time.Duration(j)*time.Millisecond)
				case 2:
					pngr.latency(p)
				case 3:
					pngr.markDisconnected(p)
				}
			}
		}(i)
	}
	wg.Wait()

	// Whatever the interleaving, a disconnected peer keeps no latency.
	for _, p := range peers {
		pngr.markDisconnected(p)
		if pngr.latency(p) != 0 {
			t.Errorf("disconnected peer %s kept latency", p)
		}
	}
}

func BenchmarkRecordLatencyIfConnected(b *testing.B) {
	pngr := newPinger(nil)
	p := peer.ID("bench-peer")
	pngr.markConnected(p)

	stop := make(chan struct{})
	go func() {
		other := peer.ID("churn-peer")
		for {
			select {
			case <-stop:
				return
			default:
				pngr.markConnected(other)
				pngr.markDisconnected(other)
			}
		}
	}()
	defer close(stop)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pngr.recordLatencyIfConnected(p, time.Millisecond)
		}
	})
}
