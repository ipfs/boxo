package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestWithResponseMetrics(t *testing.T) {
	t.Run("records all response codes", func(t *testing.T) {
		handler := withResponseMetrics(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/success":
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("ok"))
			case "/notfound":
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("not found"))
			case "/error":
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("error"))
			}
		}))

		// Make requests with different status codes
		testCases := []struct {
			path         string
			expectedCode int
		}{
			{"/success", http.StatusOK},
			{"/notfound", http.StatusNotFound},
			{"/error", http.StatusInternalServerError},
		}

		for _, tc := range testCases {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tc.expectedCode {
				t.Errorf("expected status %d for %s, got %d", tc.expectedCode, tc.path, rec.Code)
			}
		}

		// Verify metrics were recorded
		if httpResponsesTotal != nil {
			// Get metric values
			ch := make(chan prometheus.Metric, 10)
			httpResponsesTotal.Collect(ch)
			close(ch)

			foundCodes := make(map[string]bool)
			for metric := range ch {
				dto := &dto.Metric{}
				metric.Write(dto)
				if len(dto.Label) > 0 {
					code := *dto.Label[0].Value
					foundCodes[code] = true
				}
			}

			// Check that our status codes were recorded
			expectedCodes := []string{"200", "404", "500"}
			for _, code := range expectedCodes {
				if !foundCodes[code] {
					t.Errorf("expected to find metric for status code %s", code)
				}
			}
		}
	})

	t.Run("default status code is 200", func(t *testing.T) {
		handler := withResponseMetrics(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Write without calling WriteHeader
			w.Write([]byte("implicit 200"))
		}))

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected default status 200, got %d", rec.Code)
		}
	})

	t.Run("metrics wrapper preserves flusher interface", func(t *testing.T) {
		handler := withResponseMetrics(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if flusher, ok := w.(http.Flusher); ok {
				w.Write([]byte("data"))
				flusher.Flush()
			} else {
				t.Error("ResponseWriter should implement Flusher")
			}
		}))

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	})
}

func TestMiddlewareMetricsIntegration(t *testing.T) {
	t.Run("separate metrics for middleware events", func(t *testing.T) {
		// Create a handler that holds the request
		var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Hold the request for 100ms
			time.Sleep(100 * time.Millisecond)
			w.Write([]byte("ok"))
		})

		// Apply middleware in the same order as NewHandler
		handler = withConcurrentRequestLimiter(handler, 1, nil)
		handler = withResponseMetrics(handler)

		// First request should succeed
		req1 := httptest.NewRequest(http.MethodGet, "/", nil)
		rec1 := httptest.NewRecorder()
		done := make(chan bool)
		go func() {
			handler.ServeHTTP(rec1, req1)
			done <- true
		}()

		// Give first request time to start
		time.Sleep(10 * time.Millisecond)

		// Second request should be rate limited
		req2 := httptest.NewRequest(http.MethodGet, "/", nil)
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)

		if rec2.Code != http.StatusTooManyRequests {
			t.Errorf("expected status 429 for rate limited request, got %d", rec2.Code)
		}

		// Wait for first request to complete
		<-done

		// Verify HTTP response metrics were recorded
		if httpResponsesTotal != nil {
			ch := make(chan prometheus.Metric, 10)
			httpResponsesTotal.Collect(ch)
			close(ch)

			found429 := false
			for metric := range ch {
				dto := &dto.Metric{}
				metric.Write(dto)
				if len(dto.Label) > 0 {
					if *dto.Label[0].Value == "429" {
						found429 = true
					}
				}
			}
			if !found429 {
				t.Error("expected to find 429 status code in general response metrics")
			}
		}
	})

	t.Run("timeout metrics include status code", func(t *testing.T) {
		// Create a handler that times out
		var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Sleep longer than timeout
			time.Sleep(100 * time.Millisecond)
		})

		// Apply timeout middleware
		handler = withRetrievalTimeout(handler, 50*time.Millisecond, nil)
		handler = withResponseMetrics(handler)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()

		// Run in goroutine to allow timeout to trigger
		done := make(chan bool)
		go func() {
			handler.ServeHTTP(rec, req)
			done <- true
		}()

		// Wait for timeout
		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
		}

		// Verify timeout metric has code label
		if retrievalTimeouts != nil {
			ch := make(chan prometheus.Metric, 10)
			retrievalTimeouts.Collect(ch)
			close(ch)

			found504 := false
			for metric := range ch {
				dto := &dto.Metric{}
				metric.Write(dto)
				if len(dto.Label) >= 1 {
					// First label should be code
					if *dto.Label[0].Name == "code" && *dto.Label[0].Value == "504" {
						found504 = true
						// Verify other labels
						for _, label := range dto.Label {
							if *label.Name == "truncated" && *label.Value != "false" {
								t.Error("expected truncated to be 'false' for clean timeout")
							}
						}
					}
				}
			}
			if !found504 {
				t.Error("expected to find timeout metric with code '504'")
			}
		}
	})
}
