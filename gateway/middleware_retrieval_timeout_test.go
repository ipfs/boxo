package gateway

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWithRetrievalTimeout(t *testing.T) {
	t.Run("returns HTML error when Accept header includes text/html", func(t *testing.T) {
		config := &Config{DisableHTMLErrors: false}
		handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Don't write anything - simulate stuck retrieval
			time.Sleep(100 * time.Millisecond)
		}), 50*time.Millisecond, config)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Accept", "text/html,application/xhtml+xml")
		rec := httptest.NewRecorder()

		done := make(chan bool)
		go func() {
			handler.ServeHTTP(rec, req)
			done <- true
		}()

		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
		}

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
	t.Run("timeout on initial retrieval block", func(t *testing.T) {
		// Simulate a handler that blocks indefinitely on initial retrieval
		// (e.g., searching for providers that don't exist)
		handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Block forever - simulating stuck provider search
			select {}
		}), 50*time.Millisecond, nil)

		req := httptest.NewRequest(http.MethodGet, "/ipfs/bafkreif6lrhgz3fpiwypdk65qrqiey7svgpggruhbylrgv32l3izkqpsc4", nil)
		rec := httptest.NewRecorder()

		done := make(chan bool)
		go func() {
			handler.ServeHTTP(rec, req)
			done <- true
		}()

		// Wait for timeout to trigger
		select {
		case <-done:
			// Good, handler completed (due to timeout)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("handler did not complete within expected time")
		}

		if rec.Code != http.StatusGatewayTimeout {
			t.Errorf("expected status 504, got %d", rec.Code)
		}
		body := rec.Body.String()
		if !bytes.Contains([]byte(body), []byte("Unable to retrieve content within timeout period")) {
			t.Errorf("expected timeout message in body, got %s", body)
		}
	})

	t.Run("timeout on slow initial retrieval", func(t *testing.T) {
		// Simulate a handler that takes too long to retrieve initial data
		handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Simulate slow provider discovery/block retrieval
			time.Sleep(100 * time.Millisecond)
			// This write should never happen due to timeout
			w.Write([]byte("should not appear"))
		}), 50*time.Millisecond, nil)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()

		done := make(chan bool)
		go func() {
			handler.ServeHTTP(rec, req)
			done <- true
		}()

		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("handler did not complete within expected time")
		}

		if rec.Code != http.StatusGatewayTimeout {
			t.Errorf("expected status 504, got %d", rec.Code)
		}
		body := rec.Body.String()
		if bytes.Contains([]byte(body), []byte("should not appear")) {
			t.Errorf("body should not contain 'should not appear', got %s", body)
		}
	})
	t.Run("timeout disabled with zero value", func(t *testing.T) {
		handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.Write([]byte("success"))
		}), 0, nil)

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
		}), 50*time.Millisecond, nil)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()

		// Run in goroutine to allow timeout to trigger
		done := make(chan bool)
		go func() {
			handler.ServeHTTP(rec, req)
			done <- true
		}()

		// Wait for handler to complete (including timeout)
		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
		}

		if rec.Code != http.StatusGatewayTimeout {
			t.Errorf("expected status 504, got %d", rec.Code)
		}
	})

	t.Run("timeout resets on data write", func(t *testing.T) {
		handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Write data in chunks, each write should reset the timeout
			for i := 0; i < 3; i++ {
				w.Write([]byte(fmt.Sprintf("chunk%d", i)))
				w.(http.Flusher).Flush()
				time.Sleep(75 * time.Millisecond) // Less than timeout
			}
		}), 100*time.Millisecond, nil)

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
		}), 100*time.Millisecond, nil)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		// Wait for timeout
		time.Sleep(200 * time.Millisecond)

		// The initial write should succeed, but timeout should trigger after
		body := rec.Body.String()
		if !bytes.Contains([]byte(body), []byte("initial")) {
			t.Errorf("expected body to contain 'initial', got %s", body)
		}
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
		}), 50*time.Millisecond, nil)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()

		done := make(chan bool)
		go func() {
			handler.ServeHTTP(rec, req)
			done <- true
		}()

		select {
		case <-done:
		case <-time.After(300 * time.Millisecond):
		}

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

	t.Run("tracks status code for truncated responses", func(t *testing.T) {
		handler := withRetrievalTimeout(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusPartialContent) // 206
			w.Write([]byte("partial"))
			w.(http.Flusher).Flush()
			time.Sleep(150 * time.Millisecond)
		}), 50*time.Millisecond, nil)

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()

		done := make(chan bool)
		go func() {
			handler.ServeHTTP(rec, req)
			done <- true
		}()

		select {
		case <-done:
		case <-time.After(300 * time.Millisecond):
		}

		// The recorder should have captured the original status
		if rec.Code != http.StatusPartialContent {
			t.Errorf("expected status 206, got %d", rec.Code)
		}
	})
}
