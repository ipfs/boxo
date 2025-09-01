package retrieval

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetrievalState(t *testing.T) {
	t.Run("NewRetrievalState initializes correctly", func(t *testing.T) {
		rs := NewRetrievalState()
		assert.NotNil(t, rs)
		assert.Equal(t, PhaseInitializing, rs.GetPhase())
		assert.Equal(t, int32(0), rs.ProvidersFound.Load())
		assert.Equal(t, int32(0), rs.ProvidersAttempted.Load())
		assert.Equal(t, int32(0), rs.ProvidersConnected.Load())
		assert.Empty(t, rs.GetFailedProviders())
	})

	t.Run("SetPhase updates phase correctly", func(t *testing.T) {
		rs := NewRetrievalState()

		rs.SetPhase(PhasePathResolution)
		assert.Equal(t, PhasePathResolution, rs.GetPhase())

		rs.SetPhase(PhaseProviderDiscovery)
		assert.Equal(t, PhaseProviderDiscovery, rs.GetPhase())

		rs.SetPhase(PhaseConnecting)
		assert.Equal(t, PhaseConnecting, rs.GetPhase())

		rs.SetPhase(PhaseDataRetrieval)
		assert.Equal(t, PhaseDataRetrieval, rs.GetPhase())
	})

	t.Run("SetPhase enforces monotonic progression", func(t *testing.T) {
		rs := NewRetrievalState()

		// Start at initializing
		assert.Equal(t, PhaseInitializing, rs.GetPhase())

		// Move forward to connecting
		rs.SetPhase(PhaseConnecting)
		assert.Equal(t, PhaseConnecting, rs.GetPhase())

		// Try to go backward - should not change
		rs.SetPhase(PhaseProviderDiscovery)
		assert.Equal(t, PhaseConnecting, rs.GetPhase(), "Stage should not move backward")

		// Try to set same phase - should not change
		rs.SetPhase(PhaseConnecting)
		assert.Equal(t, PhaseConnecting, rs.GetPhase())

		// Can still move forward
		rs.SetPhase(PhaseDataRetrieval)
		assert.Equal(t, PhaseDataRetrieval, rs.GetPhase())

		// Try to go back again - should not change
		rs.SetPhase(PhaseInitializing)
		assert.Equal(t, PhaseDataRetrieval, rs.GetPhase(), "Stage should not move backward from data retrieval")
	})

	t.Run("Provider stats are tracked correctly", func(t *testing.T) {
		rs := NewRetrievalState()

		rs.ProvidersFound.Add(3)
		assert.Equal(t, int32(3), rs.ProvidersFound.Load())

		rs.ProvidersAttempted.Add(2)
		assert.Equal(t, int32(2), rs.ProvidersAttempted.Load())

		rs.ProvidersConnected.Add(1)
		assert.Equal(t, int32(1), rs.ProvidersConnected.Load())
	})

	t.Run("Failed providers are tracked up to limit", func(t *testing.T) {
		rs := NewRetrievalState()

		// Create real peer IDs for testing
		peerIDs := make([]peer.ID, 5)
		for i := range peerIDs {
			peerIDs[i] = test.RandPeerIDFatal(t)
		}

		// Add more than MaxFailedProvidersToTrack providers
		for _, peerID := range peerIDs {
			rs.AddFailedProvider(peerID)
		}

		failedProviders := rs.GetFailedProviders()
		assert.Len(t, failedProviders, MaxFailedProvidersToTrack)
		assert.Equal(t, peerIDs[0], failedProviders[0])
		assert.Equal(t, peerIDs[1], failedProviders[1])
		assert.Equal(t, peerIDs[2], failedProviders[2])
	})

	t.Run("Summary generates correct messages", func(t *testing.T) {
		tests := []struct {
			name              string
			setup             func(*RetrievalState)
			expectedSubstring string
		}{
			{
				name: "No providers found",
				setup: func(rs *RetrievalState) {
					rs.SetPhase(PhaseProviderDiscovery)
				},
				expectedSubstring: "no providers found for the CID",
			},
			{
				name: "Providers found but none contacted",
				setup: func(rs *RetrievalState) {
					rs.ProvidersFound.Store(5)
					rs.SetPhase(PhaseConnecting)
				},
				expectedSubstring: "found 5 provider(s) but none could be contacted",
			},
			{
				name: "Providers attempted but none reachable",
				setup: func(rs *RetrievalState) {
					rs.ProvidersFound.Store(5)
					rs.ProvidersAttempted.Store(3)
					rs.SetPhase(PhaseConnecting)
				},
				expectedSubstring: "found 5 provider(s), attempted 3, but none were reachable",
			},
			{
				name: "Providers attempted but none reachable with failed peers",
				setup: func(rs *RetrievalState) {
					rs.ProvidersFound.Store(5)
					rs.ProvidersAttempted.Store(3)
					// Store peer IDs so we can verify they appear in the message
					peerID1 := test.RandPeerIDFatal(t)
					peerID2 := test.RandPeerIDFatal(t)
					rs.AddFailedProvider(peerID1)
					rs.AddFailedProvider(peerID2)
					rs.SetPhase(PhaseConnecting)

					// Verify the summary includes the actual peer IDs
					summary := rs.Summary()
					assert.Contains(t, summary, "failed peers:")
					assert.Contains(t, summary, peerID1.String())
					assert.Contains(t, summary, peerID2.String())
				},
				expectedSubstring: "found 5 provider(s), attempted 3, but none were reachable",
			},
			{
				name: "Providers connected but didn't return content",
				setup: func(rs *RetrievalState) {
					rs.ProvidersFound.Store(5)
					rs.ProvidersAttempted.Store(3)
					rs.ProvidersConnected.Store(2)
					// Use real peer IDs
					rs.AddFailedProvider(test.RandPeerIDFatal(t))
					rs.AddFailedProvider(test.RandPeerIDFatal(t))
					rs.SetPhase(PhaseDataRetrieval)
				},
				expectedSubstring: "connected to 2, but they did not return the requested content",
			},
			{
				name: "Timeout with successful connections",
				setup: func(rs *RetrievalState) {
					rs.ProvidersFound.Store(5)
					rs.ProvidersAttempted.Store(3)
					rs.ProvidersConnected.Store(2)
					rs.SetPhase(PhaseDataRetrieval)
				},
				expectedSubstring: "timeout occurred after finding 5 provider(s) and connecting to 2",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				rs := NewRetrievalState()
				tt.setup(rs)
				summary := rs.Summary()
				assert.Contains(t, summary, tt.expectedSubstring)
				assert.Contains(t, summary, "(phase:")
			})
		}
	})

	t.Run("Context integration", func(t *testing.T) {
		// Test ContextWithState
		ctx := context.Background()
		ctxWithState, rs := ContextWithState(ctx)
		require.NotNil(t, rs)

		// Test StateFromContext retrieval
		retrievedRS := StateFromContext(ctxWithState)
		assert.Equal(t, rs, retrievedRS)

		// Test StateFromContext on context without timeout
		assert.Nil(t, StateFromContext(context.Background()))

		// Test that modifications are visible through context
		rs.ProvidersFound.Store(10)
		rs.SetPhase(PhaseDataRetrieval)

		retrievedRS = StateFromContext(ctxWithState)
		assert.Equal(t, int32(10), retrievedRS.ProvidersFound.Load())
		assert.Equal(t, PhaseDataRetrieval, retrievedRS.GetPhase())
	})

	t.Run("RetrievalPhase String method", func(t *testing.T) {
		assert.Equal(t, "initializing", PhaseInitializing.String())
		assert.Equal(t, "path resolution", PhasePathResolution.String())
		assert.Equal(t, "provider discovery", PhaseProviderDiscovery.String())
		assert.Equal(t, "connecting to providers", PhaseConnecting.String())
		assert.Equal(t, "data retrieval", PhaseDataRetrieval.String())
		assert.Equal(t, "unknown", RetrievalPhase(999).String())
	})

	t.Run("Summary includes failed peer IDs", func(t *testing.T) {
		rs := NewRetrievalState()
		rs.ProvidersFound.Store(5)
		rs.ProvidersAttempted.Store(3)
		rs.ProvidersConnected.Store(2)

		// Create real peer IDs for testing
		peerID1 := test.RandPeerIDFatal(t)
		peerID2 := test.RandPeerIDFatal(t)

		// Add some failed providers
		rs.AddFailedProvider(peerID1)
		rs.AddFailedProvider(peerID2)

		summary := rs.Summary()
		t.Logf("Summary: %s", summary)
		assert.Contains(t, summary, "failed peers:")
		// Check that at least one of the peer IDs is in the summary
		hasID1 := assert.Contains(t, summary, peerID1.String())
		hasID2 := assert.Contains(t, summary, peerID2.String())
		assert.True(t, hasID1 || hasID2, "Summary should contain at least one of the peer IDs")
	})

	t.Run("SetPhase is thread-safe and maintains monotonic ordering", func(t *testing.T) {
		rs := NewRetrievalState()
		stages := []RetrievalPhase{
			PhasePathResolution,
			PhaseProviderDiscovery,
			PhaseConnecting,
			PhaseDataRetrieval,
		}

		// Run multiple goroutines trying to set stages in various orders
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				// Each goroutine tries to set a random phase
				phase := stages[i%len(stages)]
				rs.SetPhase(phase)

				// Also try to go backward sometimes
				if i%3 == 0 {
					rs.SetPhase(PhaseInitializing)
				}
			}(i)
		}
		wg.Wait()

		// Final phase should be the highest one that was set
		finalStage := rs.GetPhase()
		assert.Equal(t, PhaseDataRetrieval, finalStage, "Should end at highest phase attempted")
	})
}

func TestErrorWithState(t *testing.T) {
	t.Run("ErrorWithState formats correctly - no providers", func(t *testing.T) {
		baseErr := errors.New("block not found")
		rs := NewRetrievalState()
		rs.SetPhase(PhaseProviderDiscovery)

		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextKey, rs)
		err := WrapWithState(ctx, baseErr)

		// Check exact error formatting for no providers
		expected := "block not found: retrieval: no providers found for the CID (phase: provider discovery)"
		assert.Equal(t, expected, err.Error())
	})

	t.Run("ErrorWithState formats correctly - providers found but none contacted", func(t *testing.T) {
		baseErr := errors.New("block not found")
		rs := NewRetrievalState()
		rs.ProvidersFound.Store(5)
		rs.SetPhase(PhaseConnecting)

		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextKey, rs)
		err := WrapWithState(ctx, baseErr)

		// Check exact error formatting
		expected := "block not found: retrieval: found 5 provider(s) but none could be contacted (phase: connecting to providers)"
		assert.Equal(t, expected, err.Error())
	})

	t.Run("ErrorWithState formats correctly - providers attempted but none reachable", func(t *testing.T) {
		baseErr := errors.New("block not found")
		rs := NewRetrievalState()
		rs.ProvidersFound.Store(3)
		rs.ProvidersAttempted.Store(2)
		rs.SetPhase(PhaseConnecting)

		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextKey, rs)
		err := WrapWithState(ctx, baseErr)

		// Check exact error formatting
		expected := "block not found: retrieval: found 3 provider(s), attempted 2, but none were reachable (phase: connecting to providers)"
		assert.Equal(t, expected, err.Error())
	})

	t.Run("ErrorWithState formats correctly - providers connected but didn't return content", func(t *testing.T) {
		baseErr := errors.New("timeout")
		rs := NewRetrievalState()
		rs.ProvidersFound.Store(5)
		rs.ProvidersAttempted.Store(3)
		rs.ProvidersConnected.Store(2)
		rs.SetPhase(PhaseDataRetrieval)

		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextKey, rs)
		err := WrapWithState(ctx, baseErr)

		// Check exact error formatting
		expected := "timeout: retrieval: timeout occurred after finding 5 provider(s) and connecting to 2 (phase: data retrieval)"
		assert.Equal(t, expected, err.Error())
	})

	t.Run("ErrorWithState formats correctly - with failed peers", func(t *testing.T) {
		baseErr := errors.New("connection failed")
		rs := NewRetrievalState()
		rs.ProvidersFound.Store(3)
		rs.ProvidersAttempted.Store(3)
		rs.ProvidersConnected.Store(1)
		rs.SetPhase(PhaseDataRetrieval)

		// Add specific peer IDs for predictable output
		peerID1 := test.RandPeerIDFatal(t)
		peerID2 := test.RandPeerIDFatal(t)
		rs.AddFailedProvider(peerID1)
		rs.AddFailedProvider(peerID2)

		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextKey, rs)
		err := WrapWithState(ctx, baseErr)

		// Check that error includes failed peers
		errMsg := err.Error()
		assert.Contains(t, errMsg, "connection failed: retrieval: found 3 provider(s), connected to 1, but they did not return the requested content (phase: data retrieval, failed peers: [")
		assert.Contains(t, errMsg, peerID1.String())
		assert.Contains(t, errMsg, peerID2.String())
	})

	t.Run("ErrorWithState formats correctly - path resolution phase", func(t *testing.T) {
		baseErr := errors.New("failed to resolve")
		rs := NewRetrievalState()
		rs.SetPhase(PhasePathResolution)

		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextKey, rs)
		err := WrapWithState(ctx, baseErr)

		// Check exact error formatting
		expected := "failed to resolve: retrieval: no providers found for the CID (phase: path resolution)"
		assert.Equal(t, expected, err.Error())
	})

	t.Run("ErrorWithState formats correctly - initializing phase", func(t *testing.T) {
		baseErr := errors.New("not started")
		rs := NewRetrievalState()
		// Phase is PhaseInitializing by default

		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextKey, rs)
		err := WrapWithState(ctx, baseErr)

		// Check exact error formatting
		expected := "not started: retrieval: no providers found for the CID (phase: initializing)"
		assert.Equal(t, expected, err.Error())
	})

	t.Run("ErrorWithState unwraps correctly", func(t *testing.T) {
		baseErr := errors.New("base error")
		rs := NewRetrievalState()

		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextKey, rs)
		err := WrapWithState(ctx, baseErr)

		// Test Unwrap
		assert.Equal(t, baseErr, errors.Unwrap(err))

		// Test errors.Is with underlying error
		assert.True(t, errors.Is(err, baseErr))
	})

	t.Run("ErrorWithState Is() method works", func(t *testing.T) {
		baseErr := errors.New("base error")
		rs := NewRetrievalState()

		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextKey, rs)
		err1 := WrapWithState(ctx, baseErr)

		// Test errors.Is with ErrorWithState type
		assert.True(t, errors.Is(err1, &ErrorWithState{}), "errors.Is should work with ErrorWithState")

		// Test that it doesn't match other error types
		otherErr := errors.New("other")
		assert.False(t, errors.Is(err1, otherErr), "Should not match non-ErrorWithState errors")
	})

	t.Run("ErrorWithState RetrievalState getter works", func(t *testing.T) {
		rs := NewRetrievalState()
		rs.ProvidersFound.Store(5)

		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextKey, rs)
		wrappedErr := WrapWithState(ctx, errors.New("test"))

		var err *ErrorWithState
		require.True(t, errors.As(wrappedErr, &err))

		retrievedState := err.RetrievalState()
		assert.Equal(t, rs, retrievedState)
		assert.Equal(t, int32(5), retrievedState.ProvidersFound.Load())
	})

	t.Run("WrapWithState wraps error with retrieval state", func(t *testing.T) {
		ctx, rs := ContextWithState(context.Background())
		rs.ProvidersFound.Store(3)
		rs.SetPhase(PhaseProviderDiscovery)

		baseErr := errors.New("connection failed")
		wrappedErr := WrapWithState(ctx, baseErr)

		// Check that it's wrapped
		var errWithState *ErrorWithState
		require.True(t, errors.As(wrappedErr, &errWithState))
		assert.Equal(t, baseErr, errors.Unwrap(errWithState))
		assert.Equal(t, rs, errWithState.RetrievalState())

		// Check formatting
		assert.Contains(t, wrappedErr.Error(), "connection failed: retrieval:")
		assert.Contains(t, wrappedErr.Error(), "found 3 provider(s)")
	})

	t.Run("WrapWithState returns nil for nil error", func(t *testing.T) {
		ctx, _ := ContextWithState(context.Background())
		assert.Nil(t, WrapWithState(ctx, nil))
	})

	t.Run("WrapWithState returns original error if no state in context", func(t *testing.T) {
		ctx := context.Background()
		baseErr := errors.New("test error")
		wrappedErr := WrapWithState(ctx, baseErr)

		// Should return the same error
		assert.Equal(t, baseErr, wrappedErr)

		// Should NOT be wrapped
		var errWithState *ErrorWithState
		assert.False(t, errors.As(wrappedErr, &errWithState))
	})

	t.Run("WrapWithState doesn't double-wrap", func(t *testing.T) {
		ctx, rs := ContextWithState(context.Background())
		rs.ProvidersFound.Store(3)

		baseErr := errors.New("test error")

		// First wrap
		wrappedOnce := WrapWithState(ctx, baseErr)

		// Try to wrap again
		wrappedTwice := WrapWithState(ctx, wrappedOnce)

		// Should be the same object
		assert.Equal(t, wrappedOnce, wrappedTwice)
	})

	t.Run("WrapWithState always wraps when state exists", func(t *testing.T) {
		ctx, rs := ContextWithState(context.Background())
		// Even with no providers found, it should wrap
		assert.Equal(t, int32(0), rs.ProvidersFound.Load())

		baseErr := errors.New("no content")
		wrappedErr := WrapWithState(ctx, baseErr)

		var errWithState *ErrorWithState
		require.True(t, errors.As(wrappedErr, &errWithState))
		assert.Contains(t, wrappedErr.Error(), "no content: retrieval:")
		assert.Contains(t, wrappedErr.Error(), "no providers found")
	})

	t.Run("ErrorWithState works with errors.As", func(t *testing.T) {
		ctx, rs := ContextWithState(context.Background())
		rs.ProvidersFound.Store(2)
		rs.AddFailedProvider(test.RandPeerIDFatal(t))

		baseErr := errors.New("fetch failed")
		wrappedErr := WrapWithState(ctx, baseErr)

		// Test errors.As
		var errWithState *ErrorWithState
		require.True(t, errors.As(wrappedErr, &errWithState))

		// Access state through the getter
		state := errWithState.RetrievalState()
		assert.Equal(t, int32(2), state.ProvidersFound.Load())
		assert.Len(t, state.GetFailedProviders(), 1)
	})
}
