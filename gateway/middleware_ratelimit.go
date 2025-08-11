package gateway

import (
	"fmt"
	"net/http"
	"strconv"
)

const retryAfterSeconds = 60 // seconds - industry standard minimum

// withConcurrentRequestLimiter limits concurrent requests using a semaphore.
// This protects the backend from overload caused by expensive operations.
//
// Why semaphore instead of rate limiting:
//   - IPFS request costs vary wildly (simple fetch vs large DAG traversal)
//   - Semaphore limits concurrent load regardless of request cost
//   - More appropriate than requests/second for resource protection
//
// Returns 429 (Too Many Requests) when at capacity:
//   - 429 indicates client-side issue (too many requests)
//   - 503 would incorrectly indicate server-side issue
//   - Industry standard for rate limiting scenarios
//
// Retry-After is fixed at 60 seconds:
//   - Industry standard minimum retry period
//   - Simple and predictable for clients
//   - Avoids complexity of dynamic calculations
//
// Parameters:
//   - handler: The HTTP handler to wrap with concurrent request limiting
//   - limit: Maximum number of concurrent requests allowed (0 disables limiting)
//   - c: Optional configuration for controlling error page rendering (can be nil)
func withConcurrentRequestLimiter(handler http.Handler, limit int, c *Config, metrics *middlewareMetrics) http.Handler {
	if limit <= 0 {
		return handler
	}

	// Create semaphore (buffered channel)
	semaphore := make(chan struct{}, limit)
	for i := 0; i < limit; i++ {
		semaphore <- struct{}{}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-semaphore:
			// Acquired a slot
			metrics.incConcurrentRequests()
			defer func() {
				semaphore <- struct{}{} // Release slot
				metrics.decConcurrentRequests()
			}()
			handler.ServeHTTP(w, r)

		default:
			// At capacity - reject with 429
			metrics.recordResponse(http.StatusTooManyRequests)
			// No need for separate rate limits metric - 429 responses ONLY come from this middleware

			// Prevent caching of rate limit responses
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))

			message := fmt.Sprintf("Too many requests. Please retry after %d seconds.", retryAfterSeconds)
			writeErrorResponse(w, r, c, http.StatusTooManyRequests, message)
		}
	})
}
