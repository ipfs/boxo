//go:build go1.25

package gateway

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"
)

func TestWithRetrievalTimeout(t *testing.T) {
	t.Run("returns HTML error when Accept header includes text/html", func(t *testing.T) {
		config := &Config{DisableHTMLErrors: false}
		handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Don't write anything - simulate stuck retrieval
			time.Sleep(100 * time.Millisecond)
		}), 50*time.Millisecond, config, newTestMetrics())

		synctest.Test(t, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Accept", "text/html,application/xhtml+xml")
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)
			time.Sleep(200 * time.Millisecond) // let internal goroutines complete before bubble exits
			synctest.Wait()

			if rec.Code != http.StatusGatewayTimeout {
				t.Errorf("expected status 504, got %d", rec.Code)
			}

			contentType := rec.Header().Get("Content-Type")
			if contentType != "text/html" {
				t.Errorf("expected Content-Type text/html, got %s", contentType)
			}

			body := rec.Body.String()
			if !bytes.Contains([]byte(body), []byte("<html")) {
				t.Error("expected HTML response body")
			}
			if !bytes.Contains([]byte(body), []byte("504")) {
				t.Error("expected 504 in HTML body")
			}
		})
	})
	t.Run("timeout on initial retrieval block", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			blockChan := make(chan struct{})

			// Simulate a handler that blocks indefinitely on initial retrieval
			// (e.g., searching for providers that don't exist)
			handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Block until test cleanup - simulating stuck provider search
				<-blockChan
			}), 50*time.Millisecond, nil, newTestMetrics())

			req := httptest.NewRequest(http.MethodGet, "/ipfs/bafkreif6lrhgz3fpiwypdk65qrqiey7svgpggruhbylrgv32l3izkqpsc4", nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusGatewayTimeout {
				t.Errorf("expected status 504, got %d", rec.Code)
			}
			body := rec.Body.String()
			if !bytes.Contains([]byte(body), []byte("Unable to retrieve content within timeout period")) {
				t.Errorf("expected timeout message in body, got %s", body)
			}

			// Allow blocked handler goroutine to exit cleanly
			close(blockChan)
			time.Sleep(100 * time.Millisecond) // let internal goroutines complete before bubble exits
			synctest.Wait()
		})
	})

	t.Run("timeout on slow initial retrieval", func(t *testing.T) {
		// Simulate a handler that takes too long to retrieve initial data
		handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Simulate slow provider discovery/block retrieval
			time.Sleep(100 * time.Millisecond)
			// This write should never happen due to timeout
			w.Write([]byte("should not appear"))
		}), 50*time.Millisecond, nil, newTestMetrics())

		synctest.Test(t, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)
			time.Sleep(200 * time.Millisecond) // let internal goroutines complete before bubble exits
			synctest.Wait()

			if rec.Code != http.StatusGatewayTimeout {
				t.Errorf("expected status 504, got %d", rec.Code)
			}
			body := rec.Body.String()
			if bytes.Contains([]byte(body), []byte("should not appear")) {
				t.Errorf("body should not contain 'should not appear', got %s", body)
			}
		})
	})
	t.Run("timeout disabled with zero value", func(t *testing.T) {
		handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.Write([]byte("success"))
		}), 0, nil, newTestMetrics())

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rec.Code)
		}
		if rec.Body.String() != "success" {
			t.Errorf("expected body 'success', got %s", rec.Body.String())
		}
	})

	t.Run("timeout triggers after no data", func(t *testing.T) {
		handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Don't write anything for longer than timeout
			time.Sleep(150 * time.Millisecond)
		}), 50*time.Millisecond, nil, newTestMetrics())

		synctest.Test(t, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)
			time.Sleep(200 * time.Millisecond) // let internal goroutines complete before bubble exits
			synctest.Wait()

			if rec.Code != http.StatusGatewayTimeout {
				t.Errorf("expected status 504, got %d", rec.Code)
			}
		})
	})

	t.Run("timeout resets on data write", func(t *testing.T) {
		handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Write data in chunks, each write should reset the timeout
			for i := 0; i < 3; i++ {
				w.Write([]byte(fmt.Sprintf("chunk%d", i)))
				w.(http.Flusher).Flush()
				time.Sleep(75 * time.Millisecond) // Less than timeout
			}
		}), 100*time.Millisecond, nil, newTestMetrics())

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rec.Code)
		}
		expected := "chunk0chunk1chunk2"
		if rec.Body.String() != expected {
			t.Errorf("expected body '%s', got %s", expected, rec.Body.String())
		}
	})

	t.Run("empty writes don't reset timeout", func(t *testing.T) {
		handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("initial"))
			// Empty writes shouldn't reset timeout
			for i := 0; i < 5; i++ {
				w.Write([]byte{})
				time.Sleep(30 * time.Millisecond)
			}
		}), 100*time.Millisecond, nil, newTestMetrics())

		synctest.Test(t, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)
			time.Sleep(300 * time.Millisecond) // let internal goroutines complete before bubble exits
			synctest.Wait()

			// The initial write should succeed, but timeout should trigger after
			body := rec.Body.String()
			if !bytes.Contains([]byte(body), []byte("initial")) {
				t.Errorf("expected body to contain 'initial', got %s", body)
			}
		})
	})

	t.Run("truncation message on mid-stream timeout", func(t *testing.T) {
		handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("partial data"))
			w.(http.Flusher).Flush()
			// Now sleep longer than timeout
			time.Sleep(150 * time.Millisecond)
			// This write should fail
			w.Write([]byte("should not appear"))
		}), 50*time.Millisecond, nil, newTestMetrics())

		synctest.Test(t, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)
			time.Sleep(300 * time.Millisecond) // let internal goroutines complete before bubble exits
			synctest.Wait()

			body := rec.Body.String()
			// Should contain the initial data
			if !bytes.Contains([]byte(body), []byte("partial data")) {
				t.Errorf("expected body to contain 'partial data', got %s", body)
			}
			// Should contain truncation message
			if !bytes.Contains([]byte(body), []byte(truncationMessage)) {
				t.Errorf("expected body to contain truncation message, got %s", body)
			}
			// Should not contain data after timeout
			if bytes.Contains([]byte(body), []byte("should not appear")) {
				t.Errorf("body should not contain 'should not appear', got %s", body)
			}
		})
	})

	t.Run("tracks status code for truncated responses", func(t *testing.T) {
		handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusPartialContent) // 206
			w.Write([]byte("partial"))
			w.(http.Flusher).Flush()
			time.Sleep(150 * time.Millisecond)
		}), 50*time.Millisecond, nil, newTestMetrics())

		synctest.Test(t, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)
			time.Sleep(300 * time.Millisecond) // let internal goroutines complete before bubble exits
			synctest.Wait()

			// The recorder should have captured the original status
			if rec.Code != http.StatusPartialContent {
				t.Errorf("expected status 206, got %d", rec.Code)
			}
		})
	})
}

// TestConcurrentHeaderAccessRace verifies that no "concurrent map writes" panic
// occurs when the timeout handler writes an error response while the request
// handler is still trying to modify headers.
//
// The race condition occurs when both the timeout goroutine and the handler
// goroutine try to access the same underlying http.Header map. Without proper
// isolation, this causes a "concurrent map writes" panic.
//
// This test is designed to be run with the race detector:
//
//	go test -race -run TestConcurrentHeaderAccessRace ./gateway
//
// For more thorough testing with multiple iterations:
//
//	go test -shuffle=on -count=10 -failfast -race -parallel 8 -run TestConcurrentHeaderAccessRace ./gateway -v
//
// The race is triggered by having the handler continuously modify headers
// while a timeout occurs, causing both goroutines to access the same header
// map if not properly isolated.
func TestConcurrentHeaderAccessRace(t *testing.T) {
	t.Run("race before headers sent (504 response)", func(t *testing.T) {
		// Handler must run longer than timeout for race window to exist.
		// With fake time: 100 × 100µs = 10ms handler vs 5ms timeout ensures
		// timeout fires while handler is still modifying headers.
		const iterations = 100
		const sleepPerIter = 100 * time.Microsecond // total: 10ms
		const timeout = 5 * time.Millisecond

		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for i := 0; i < iterations; i++ {
				w.Header().Set("X-Test", fmt.Sprintf("value-%d", i))
				time.Sleep(sleepPerIter)
			}
		})

		config := &Config{DisableHTMLErrors: true}
		timeoutHandler := withRetrievalTimeout(handler, timeout, config, newTestMetrics())

		// Run with race detector
		for i := 0; i < 5; i++ {
			synctest.Test(t, func(t *testing.T) {
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "/test", nil)

				var panicOccurred bool
				func() {
					defer func() {
						if r := recover(); r != nil {
							// Check if it's the concurrent map writes panic
							if panicStr, ok := r.(string); ok && panicStr == "concurrent map writes" {
								panicOccurred = true
							}
						}
					}()
					timeoutHandler.ServeHTTP(rec, req)
				}()

				time.Sleep(20 * time.Millisecond) // let internal goroutines complete before bubble exits
				synctest.Wait()

				if panicOccurred {
					t.Logf("Reproduced concurrent map writes panic!")
				}
			})
		}
	})

	t.Run("race after headers sent (200 response truncation)", func(t *testing.T) {
		// Test the race condition when timeout occurs after headers are already sent.
		// This tests the truncation path where we write truncation message and hijack connection.

		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Write headers and some data first
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("initial data"))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}

			// Then continue modifying headers (which should be no-op after WriteHeader)
			// and writing more data slowly to trigger timeout during response
			for i := 0; i < 100; i++ {
				// Try to set headers after WriteHeader (should be ignored but could race)
				w.Header().Set("X-Late-Header", fmt.Sprintf("value-%d", i))

				// Write small chunks of data
				if i%10 == 0 {
					w.Write([]byte(fmt.Sprintf("chunk-%d", i)))
					if f, ok := w.(http.Flusher); ok {
						f.Flush()
					}
				}
				time.Sleep(time.Millisecond)
			}
		})

		config := &Config{DisableHTMLErrors: true}
		timeoutHandler := withRetrievalTimeout(handler, 20*time.Millisecond, config, newTestMetrics())

		// Run with race detector
		for i := 0; i < 5; i++ {
			synctest.Test(t, func(t *testing.T) {
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(http.MethodGet, "/test", nil)

				var panicOccurred bool
				func() {
					defer func() {
						if r := recover(); r != nil {
							// Check if it's the concurrent map writes panic
							if panicStr, ok := r.(string); ok && panicStr == "concurrent map writes" {
								panicOccurred = true
							}
						}
					}()
					timeoutHandler.ServeHTTP(rec, req)
				}()

				time.Sleep(200 * time.Millisecond) // let internal goroutines complete before bubble exits
				synctest.Wait()

				if panicOccurred {
					t.Errorf("Reproduced concurrent map writes panic in truncation path!")
				}

				// Verify we got initial data before timeout
				body := rec.Body.String()
				if !bytes.Contains([]byte(body), []byte("initial data")) {
					t.Errorf("Expected initial data in response, got: %s", body)
				}
			})
		}
	})
}
