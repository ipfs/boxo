package gateway

import (
	"net/http"
	"sync/atomic"
)

// requestLimiter is a wrapper around http.Handler that limits the number of concurrent requests.
type requestLimiter struct {
	handler      http.Handler
	maxInFlight  int32
	currentCount int32
}

// defaultMaxRequestsInFlight is the default value for MaxRequestsInFlight if not specified or invalid.
const defaultMaxRequestsInFlight = 1024

// newRequestLimiter creates a new request limiter that wraps the given handler.
func newRequestLimiter(handler http.Handler, maxRequestsInFlight int) http.Handler {
	// Use default if the provided value is invalid
	max := maxRequestsInFlight
	if max <= 0 {
		max = defaultMaxRequestsInFlight
	}

	return &requestLimiter{
		handler:     handler,
		maxInFlight: int32(max),
	}
}

// ServeHTTP implements the http.Handler interface.
func (l *requestLimiter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Try to increment the counter
	current := atomic.AddInt32(&l.currentCount, 1)

	// Check if we've exceeded the limit
	if current > l.maxInFlight {
		// Decrement the counter since we're rejecting this request
		atomic.AddInt32(&l.currentCount, -1)

		// Return 503 Service Unavailable with a Retry-After header
		w.Header().Set("Retry-After", "5") // 5 seconds is a reasonable default
		http.Error(w, "Too many requests in flight", http.StatusServiceUnavailable)
		return
	}

	// Process the request and ensure we decrement the counter when done
	defer atomic.AddInt32(&l.currentCount, -1)
	l.handler.ServeHTTP(w, r)
}
