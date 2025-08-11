package gateway

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWithConcurrentRequestLimiter(t *testing.T) {
	t.Run("returns HTML error when Accept header includes text/html", func(t *testing.T) {
		config := &Config{DisableHTMLErrors: false}
		handler := withConcurrentRequestLimiter(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Block to simulate a busy handler
			time.Sleep(100 * time.Millisecond)
		}), 1, config, newTestMetrics())

		// First request occupies the slot
		req1 := httptest.NewRequest(http.MethodGet, "/", nil)
		rec1 := httptest.NewRecorder()
		go func() {
			handler.ServeHTTP(rec1, req1)
		}()

		// Give first request time to start
		time.Sleep(10 * time.Millisecond)

		// Second request with HTML accept should get HTML error
		req2 := httptest.NewRequest(http.MethodGet, "/", nil)
		req2.Header.Set("Accept", "text/html,application/xhtml+xml")
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)

		if rec2.Code != http.StatusTooManyRequests {
			t.Errorf("expected status 429, got %d", rec2.Code)
		}

		contentType := rec2.Header().Get("Content-Type")
		if contentType != "text/html" {
			t.Errorf("expected Content-Type text/html, got %s", contentType)
		}

		body := rec2.Body.String()
		if !bytes.Contains([]byte(body), []byte("<html")) {
			t.Error("expected HTML response body")
		}
		if !bytes.Contains([]byte(body), []byte("429")) {
			t.Error("expected 429 in HTML body")
		}
	})
	t.Run("limiter disabled with zero value", func(t *testing.T) {
		var requestCount atomic.Int32
		handler := withConcurrentRequestLimiter(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			time.Sleep(50 * time.Millisecond)
			w.Write([]byte("success"))
		}), 0, nil, newTestMetrics())

		// Send multiple concurrent requests
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
			}()
		}
		wg.Wait()

		if requestCount.Load() != 10 {
			t.Errorf("expected 10 requests to be processed, got %d", requestCount.Load())
		}
	})

	t.Run("limits concurrent requests", func(t *testing.T) {
		const limit = 2
		var concurrent atomic.Int32
		var maxConcurrent atomic.Int32

		handler := withConcurrentRequestLimiter(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cur := concurrent.Add(1)
			defer concurrent.Add(-1)

			// Track max concurrent requests
			for {
				max := maxConcurrent.Load()
				if cur <= max || maxConcurrent.CompareAndSwap(max, cur) {
					break
				}
			}

			time.Sleep(50 * time.Millisecond)
			w.Write([]byte("success"))
		}), limit, nil, newTestMetrics())

		// Send more requests than the limit
		var wg sync.WaitGroup
		results := make([]int, 5)
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				results[idx] = rec.Code
			}(i)
		}
		wg.Wait()

		// Check that max concurrent never exceeded limit
		if maxConcurrent.Load() > int32(limit) {
			t.Errorf("max concurrent requests %d exceeded limit %d", maxConcurrent.Load(), limit)
		}

		// Count successful and rejected requests
		successCount := 0
		rejectCount := 0
		for _, code := range results {
			if code == http.StatusOK {
				successCount++
			} else if code == http.StatusTooManyRequests {
				rejectCount++
			}
		}

		if successCount == 0 {
			t.Error("expected at least some successful requests")
		}
		if rejectCount == 0 {
			t.Error("expected at least some rejected requests when limit exceeded")
		}
	})

	t.Run("returns 429 with Retry-After and Cache-Control headers", func(t *testing.T) {
		handler := withConcurrentRequestLimiter(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.Write([]byte("success"))
		}), 1, nil, newTestMetrics())

		// First request should succeed
		req1 := httptest.NewRequest(http.MethodGet, "/", nil)
		rec1 := httptest.NewRecorder()
		go func() {
			handler.ServeHTTP(rec1, req1)
		}()

		// Give first request time to start
		time.Sleep(10 * time.Millisecond)

		// Second request should be rejected
		req2 := httptest.NewRequest(http.MethodGet, "/", nil)
		rec2 := httptest.NewRecorder()
		handler.ServeHTTP(rec2, req2)

		if rec2.Code != http.StatusTooManyRequests {
			t.Errorf("expected status 429, got %d", rec2.Code)
		}

		retryAfter := rec2.Header().Get("Retry-After")
		if retryAfter == "" {
			t.Error("expected Retry-After header to be set")
		}

		retrySeconds, err := strconv.Atoi(retryAfter)
		if err != nil {
			t.Errorf("Retry-After should be a number, got %s", retryAfter)
		}

		// Retry should be 60 seconds (industry standard minimum)
		if retrySeconds != 60 {
			t.Errorf("expected Retry-After to be 60, got %d", retrySeconds)
		}

		// Check Cache-Control header to prevent caching
		cacheControl := rec2.Header().Get("Cache-Control")
		if cacheControl != "no-store" {
			t.Errorf("expected Cache-Control: no-store, got %s", cacheControl)
		}
	})

	t.Run("Retry-After is static", func(t *testing.T) {
		handler := withConcurrentRequestLimiter(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Hold the request indefinitely
			select {}
		}), 1, nil, newTestMetrics())

		// Fill the single slot
		go func() {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
		}()

		time.Sleep(10 * time.Millisecond)

		// Generate many rejections and verify Retry-After stays constant
		for i := 0; i < 20; i++ {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			retryAfter := rec.Header().Get("Retry-After")
			if retryAfter != "60" {
				t.Errorf("expected Retry-After to be static at 60, got %s", retryAfter)
			}
		}
	})
}

func TestMiddlewareIntegration(t *testing.T) {
	t.Run("both middlewares work together", func(t *testing.T) {
		var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Simulate processing that writes data periodically
			for i := 0; i < 3; i++ {
				w.Write([]byte(fmt.Sprintf("data%d", i)))
				w.(http.Flusher).Flush()
				time.Sleep(30 * time.Millisecond)
			}
		})

		// Apply both middlewares
		metrics := newTestMetrics()
		handler = withRetrievalTimeout(handler, 50*time.Millisecond, nil, metrics)
		handler = withConcurrentRequestLimiter(handler, 2, nil, metrics)

		// Test that requests work normally within limits
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					t.Errorf("expected status 200, got %d", rec.Code)
				}
			}()
		}
		wg.Wait()
	})
}
