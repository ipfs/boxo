package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/boxo/path"
	"github.com/stretchr/testify/require"
)

// Mock backend that simulates a slow remote blockstore
type slowBackend struct {
	IPFSBackend
	delay time.Duration
}

func (sb *slowBackend) IsCached(ctx context.Context, p path.Path) bool {
	select {
	case <-time.After(sb.delay):
		return false
	case <-ctx.Done():
		// Context cancelled/timed out
		return false
	}
}

func TestOnlyIfCachedTimeoutBehavior(t *testing.T) {
	testPath, _ := path.NewPath("/ipfs/QmTest")

	testCases := []struct {
		name             string
		backendDelay     time.Duration
		retrievalTimeout time.Duration
		expectedMinTime  time.Duration
		expectedMaxTime  time.Duration
		description      string
	}{
		{
			name:             "With RetrievalTimeout set",
			backendDelay:     60 * time.Second,
			retrievalTimeout: 5 * time.Second,
			expectedMinTime:  3 * time.Second, // Should take at least 3s (60% of 80% of 5s = 2.4s, rounded up)
			expectedMaxTime:  5 * time.Second, // Should not exceed the full timeout
			description:      "Should timeout at 80% of RetrievalTimeout",
		},
		{
			name:             "With RetrievalTimeout disabled",
			backendDelay:     1 * time.Second,
			retrievalTimeout: 0,                      // Disabled
			expectedMinTime:  900 * time.Millisecond, // Should wait for backend (with some margin)
			expectedMaxTime:  2 * time.Second,        // Should complete shortly after backend responds
			description:      "Should wait for backend without timeout",
		},
		{
			name:             "Quick backend response",
			backendDelay:     10 * time.Millisecond,
			retrievalTimeout: 5 * time.Second,
			expectedMinTime:  5 * time.Millisecond,
			expectedMaxTime:  100 * time.Millisecond,
			description:      "Should return quickly when backend responds fast",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Setup
			backend := &slowBackend{delay: tc.backendDelay}
			h := &handler{
				backend: backend,
				config:  &Config{RetrievalTimeout: tc.retrievalTimeout},
			}

			req := httptest.NewRequest(http.MethodHead, "/ipfs/QmTest", nil)
			req.Header.Set("Cache-Control", "only-if-cached")
			rec := httptest.NewRecorder()

			// Execute
			start := time.Now()
			handled := h.handleOnlyIfCached(rec, req, testPath)
			elapsed := time.Since(start)

			// Verify
			require.True(t, handled, "Request should be handled")
			require.Equal(t, http.StatusPreconditionFailed, rec.Code, "Should return 412")
			require.Greater(t, elapsed, tc.expectedMinTime, tc.description)
			require.Less(t, elapsed, tc.expectedMaxTime, tc.description)
		})
	}
}

// Test that only-if-cached correctly handles different HTTP methods
func TestOnlyIfCachedMethods(t *testing.T) {
	// Use a valid CID that is not present in the cache
	testPath, _ := path.NewPath("/ipfs/bafkreicm2cerwpdtah2rd7rxg5jcaqsj52blfuaprkurromr5y6p3a5zlu")

	// Create a mock backend that returns cached=false
	backend := &slowBackend{delay: 10 * time.Millisecond}
	h := &handler{
		backend: backend,
		config:  &Config{RetrievalTimeout: 1 * time.Second},
	}

	t.Run("HEAD method returns 412 when not cached", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/ipfs/QmTest", nil)
		req.Header.Set("Cache-Control", "only-if-cached")
		rec := httptest.NewRecorder()

		handled := h.handleOnlyIfCached(rec, req, testPath)

		require.True(t, handled)
		require.Equal(t, http.StatusPreconditionFailed, rec.Code)
		require.Empty(t, rec.Body.String(), "HEAD response should have no body")
	})

	t.Run("GET method returns 412 with error message when not cached", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ipfs/QmTest", nil)
		req.Header.Set("Cache-Control", "only-if-cached")
		rec := httptest.NewRecorder()

		handled := h.handleOnlyIfCached(rec, req, testPath)

		require.True(t, handled)
		require.Equal(t, http.StatusPreconditionFailed, rec.Code)
		require.Contains(t, rec.Body.String(), "not in local datastore")
	})
}
