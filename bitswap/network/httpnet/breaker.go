package httpnet

import (
	"sync"
	"sync/atomic"
)

// breaker holds one shared serverError counter per HTTP endpoint.
//
// Without sharing, every (peer, URL) pair has its own counter and a
// flaky host can drain the per-peer quota of every peer that maps to
// it before any breaker trips. When N peer IDs resolve to one gateway
// the host accumulates N x MaxRetries attempts before all peers
// disconnect; the shared counter trips the breaker once for the whole
// gateway.
//
// Counters are returned as pointers so callers (senderURL) can use
// atomic.Int64 directly without going through the breaker on the hot
// path.
type breaker struct {
	mu       sync.Mutex
	counters map[string]*atomic.Int64
}

func newBreaker() *breaker {
	return &breaker{counters: make(map[string]*atomic.Int64)}
}

// counter returns the shared per-host serverErrors counter for key,
// allocating one on first use.
func (b *breaker) counter(key string) *atomic.Int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if c, ok := b.counters[key]; ok {
		return c
	}
	c := new(atomic.Int64)
	b.counters[key] = c
	return c
}

// reset drops the counter for key. Called when no peer is using the
// host so a future reconnect starts fresh.
func (b *breaker) reset(key string) {
	b.mu.Lock()
	delete(b.counters, key)
	b.mu.Unlock()
}
