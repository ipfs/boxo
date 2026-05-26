package httpnet

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

// TestInflightCoalescesConcurrentCallers verifies that concurrent callers
// on the same key share one underlying call and observe the same result.
func TestInflightCoalescesConcurrentCallers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracker := newInflightTracker()

		var calls atomic.Int32
		gate := make(chan struct{})
		want := &inflightResult{statusCode: 200, body: []byte("hello")}

		fn := func() *inflightResult {
			calls.Add(1)
			<-gate
			return want
		}

		const N = 10
		results := make([]*inflightResult, N)
		shareds := make([]bool, N)

		var wg sync.WaitGroup
		wg.Add(N)
		for i := range N {
			go func(i int) {
				defer wg.Done()
				results[i], shareds[i] = tracker.do("k", fn)
			}(i)
		}

		// Wait until the leader is blocked on gate and every waiter
		// is blocked on the inflightCall's done channel.
		synctest.Wait()
		close(gate)
		wg.Wait()

		if got := calls.Load(); got != 1 {
			t.Fatalf("fn called %d times, want 1", got)
		}
		leaders := 0
		for i, r := range results {
			if r != want {
				t.Fatalf("caller %d got result %v, want %v", i, r, want)
			}
			if !shareds[i] {
				leaders++
			}
		}
		if leaders != 1 {
			t.Fatalf("got %d leaders, want exactly 1", leaders)
		}
	})
}

// TestInflightDistinctKeysRunIndependently verifies that callers on
// different keys do not block each other and each runs fn.
func TestInflightDistinctKeysRunIndependently(t *testing.T) {
	tracker := newInflightTracker()

	var calls atomic.Int32
	fn := func() *inflightResult {
		calls.Add(1)
		return &inflightResult{statusCode: 200}
	}

	for _, key := range []string{"a", "b", "c"} {
		_, shared := tracker.do(key, fn)
		if shared {
			t.Fatalf("key %q reported shared=true on first call", key)
		}
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("fn called %d times, want 3", got)
	}
}

// TestInflightReleasesAfterCompletion verifies that the tracker drops
// completed entries so that subsequent identical calls run fn again
// rather than returning the stale prior result.
func TestInflightReleasesAfterCompletion(t *testing.T) {
	tracker := newInflightTracker()

	first := &inflightResult{statusCode: 200, body: []byte("a")}
	second := &inflightResult{statusCode: 404, body: []byte("b")}

	got1, shared1 := tracker.do("k", func() *inflightResult { return first })
	if shared1 || got1 != first {
		t.Fatalf("first call: shared=%v got=%v want first=%v", shared1, got1, first)
	}

	got2, shared2 := tracker.do("k", func() *inflightResult { return second })
	if shared2 || got2 != second {
		t.Fatalf("second call: shared=%v got=%v want second=%v", shared2, got2, second)
	}
}

// TestInflightPropagatesErrors verifies that a leader failure surfaces
// to all waiters identically.
func TestInflightPropagatesErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tracker := newInflightTracker()

		gate := make(chan struct{})
		wantErr := errors.New("boom")
		fn := func() *inflightResult {
			<-gate
			return &inflightResult{err: wantErr, errType: typeServer}
		}

		const N = 4
		results := make([]*inflightResult, N)
		var wg sync.WaitGroup
		wg.Add(N)
		for i := range N {
			go func(i int) {
				defer wg.Done()
				results[i], _ = tracker.do("k", fn)
			}(i)
		}
		synctest.Wait()
		close(gate)
		wg.Wait()

		for i, r := range results {
			if r == nil || !errors.Is(r.err, wantErr) {
				t.Fatalf("caller %d got %v, want err=%v", i, r, wantErr)
			}
		}
	})
}

// TestInflightKeyDistinguishesFields verifies that the key separates
// requests that differ in any field that influences the upstream
// response.
func TestInflightKeyDistinguishesFields(t *testing.T) {
	base := inflightKey("https", "example.com", "sni", http.MethodGet, "cid")
	cases := map[string]string{
		"scheme": inflightKey("http", "example.com", "sni", http.MethodGet, "cid"),
		"host":   inflightKey("https", "other.com", "sni", http.MethodGet, "cid"),
		"sni":    inflightKey("https", "example.com", "other-sni", http.MethodGet, "cid"),
		"method": inflightKey("https", "example.com", "sni", http.MethodHead, "cid"),
		"cid":    inflightKey("https", "example.com", "sni", http.MethodGet, "other-cid"),
	}
	for name, k := range cases {
		if k == base {
			t.Errorf("differing %s produced identical key", name)
		}
	}
}
