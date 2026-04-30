package httpnet

import (
	"strings"
	"sync"
)

// inflightTracker coalesces concurrent identical HTTP requests so that
// multiple peer IDs sharing one HTTP endpoint produce one round trip,
// not one per peer ID.
//
// Delegated routing returns multiple peer IDs for one HTTP gateway. For
// example, /dns/a-fil-http.aur.lu/tcp/443/https is currently advertised
// under three peer IDs. Bitswap creates one MessageQueue per peer ID, so
// a single want-have broadcast for one CID becomes three identical HEAD
// requests against the same upstream. Want-have probes that bitswap
// sends to non-best peers fan out the same way. The pattern compounds
// as HTTP retrieval grows.
//
// Coalescing applies only while a request is in flight. Once the leader
// returns, subsequent callers issue their own requests; we do not cache
// results.
type inflightTracker struct {
	mu    sync.Mutex
	calls map[string]*inflightCall
}

// inflightResult carries the response data shared from leader to waiters.
//
// On a successful round trip, statusCode, body, retryAfter, and
// requestURL are populated and err is nil. On failure, err and errType
// are populated; statusCode and body may also be set if the failure
// happened after the response arrived.
type inflightResult struct {
	statusCode int
	body       []byte
	retryAfter string
	requestURL string
	err        error
	errType    senderErrorType // valid only when err != nil
}

type inflightCall struct {
	done   chan struct{}
	result *inflightResult
}

func newInflightTracker() *inflightTracker {
	return &inflightTracker{calls: make(map[string]*inflightCall)}
}

// do runs fn once for the leader and returns the same result to every
// concurrent waiter on key. shared reports whether this caller waited on
// another leader.
//
// The key must distinguish requests that differ in any field that would
// change the upstream response: scheme, host, SNI, method, and CID. Two
// peers that agree on all five send identical bytes on the wire and may
// share the result.
func (t *inflightTracker) do(key string, fn func() *inflightResult) (result *inflightResult, shared bool) {
	t.mu.Lock()
	if c, ok := t.calls[key]; ok {
		t.mu.Unlock()
		<-c.done
		return c.result, true
	}
	c := &inflightCall{done: make(chan struct{})}
	t.calls[key] = c
	t.mu.Unlock()

	c.result = fn()

	t.mu.Lock()
	delete(t.calls, key)
	t.mu.Unlock()
	close(c.done)

	return c.result, false
}

// inflightKey builds the singleflight key. SNI is part of the key
// because providers can advertise the same host with different SNI
// expectations and the upstream response may differ.
func inflightKey(scheme, host, sni, method, cid string) string {
	var sb strings.Builder
	sb.Grow(len(scheme) + len(host) + len(sni) + len(method) + len(cid) + 7)
	sb.WriteString(scheme)
	sb.WriteString("://")
	sb.WriteString(host)
	sb.WriteByte('|')
	sb.WriteString(sni)
	sb.WriteByte('|')
	sb.WriteString(method)
	sb.WriteByte('|')
	sb.WriteString(cid)
	return sb.String()
}
