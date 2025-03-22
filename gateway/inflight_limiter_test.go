package gateway

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// TestRequestLimiter tests that the request limiter correctly limits the number of concurrent requests.
func TestRequestLimiter(t *testing.T) {
	// Create a simple handler that just returns 200 OK
	baseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Create a request limiter with a max of 2 concurrent requests
	limiter := newRequestLimiter(baseHandler, 2)

	// Test that we can make 2 concurrent requests
	wg := sync.WaitGroup{}
	blockCh := make(chan struct{})
	// Add a channel to signal when requests are being processed
	inFlightCh := make(chan struct{}, 2)

	// Create a handler that signals when it's processing a request
	blockingHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Signal that we're processing a request
		inFlightCh <- struct{}{}
		// Wait for the signal to complete
		<-blockCh
		w.WriteHeader(http.StatusOK)
	})

	// Create a limiter with our blocking handler
	limiter = newRequestLimiter(blockingHandler, 2)

	// Start 2 requests that will block until we close blockCh
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Create a test request
			req := httptest.NewRequest("GET", "http://example.com/", nil)
			rec := httptest.NewRecorder()

			// Call the limiter directly (no need for another goroutine)
			limiter.ServeHTTP(rec, req)
		}()
	}

	// Wait for both requests to be in flight
	for i := 0; i < 2; i++ {
		<-inFlightCh
	}

	// Try to make a third request, which should be rejected
	req := httptest.NewRequest("GET", "http://example.com/", nil)
	rec := httptest.NewRecorder()
	limiter.ServeHTTP(rec, req)

	// Check that the third request was rejected with 503
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status code %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}

	// Check that the Retry-After header was set
	if rec.Header().Get("Retry-After") == "" {
		t.Error("Expected Retry-After header to be set")
	}

	// Unblock the first two requests
	close(blockCh)

	// Wait for the first two requests to complete
	wg.Wait()
}

// TestRequestLimiterDefault tests that the request limiter uses the default max requests value
// when an invalid value is provided.
func TestRequestLimiterDefault(t *testing.T) {
	// Create a simple handler
	baseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Create a request limiter with an invalid max (should use default)
	limiter := newRequestLimiter(baseHandler, -1).(*requestLimiter)

	// Check that the max in flight is set to the default
	if limiter.maxInFlight != defaultMaxRequestsInFlight {
		t.Errorf("Expected max in flight to be %d, got %d", defaultMaxRequestsInFlight, limiter.maxInFlight)
	}

	// Create a request limiter with a valid max
	limiter = newRequestLimiter(baseHandler, 100).(*requestLimiter)

	// Check that the max in flight is set to the provided value
	if limiter.maxInFlight != 100 {
		t.Errorf("Expected max in flight to be %d, got %d", 100, limiter.maxInFlight)
	}
}
