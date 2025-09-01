// Package retrieval provides state tracking for IPFS content retrieval operations.
// It enables detailed diagnostics about the retrieval process, including which stage
// failed (path resolution, provider discovery, connection, or block retrieval) and
// statistics about provider interactions. This information is particularly useful
// for debugging timeout errors and understanding retrieval performance.
package retrieval

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/libp2p/go-libp2p/core/peer"
)

type contextKey string

// ContextKey is the key used to store RetrievalState in a context.Context.
// This can be used directly with context.WithValue if needed, though the
// ContextWithState and StateFromContext functions are preferred.
const ContextKey contextKey = "boxo-retrieval-state"

// MaxFailedProvidersToTrack limits the number of failed provider peer IDs
// that are kept for diagnostic purposes. This prevents unbounded memory growth
// while still providing useful debugging information.
const MaxFailedProvidersToTrack = 3

// RetrievalPhase represents the current phase of content retrieval.
// Phases progress monotonically - they can only move forward, never backward.
// This helps identify where in the retrieval process a timeout or failure occurred.
type RetrievalPhase int

const (
	// PhaseInitializing indicates the retrieval process has not yet started.
	PhaseInitializing RetrievalPhase = iota
	// PhasePathResolution indicates the system is resolving an IPFS path to determine
	// what content needs to be fetched (e.g., /ipfs/Qm.../path/to/file).
	PhasePathResolution
	// PhaseProviderDiscovery indicates the system is finding peers that have the content.
	PhaseProviderDiscovery
	// PhaseConnecting indicates the system is establishing connections to providers.
	PhaseConnecting
	// PhaseDataRetrieval indicates the system is transferring data to the client.
	PhaseDataRetrieval
)

// String returns a human-readable name for the retrieval phase.
func (p RetrievalPhase) String() string {
	switch p {
	case PhaseInitializing:
		return "initializing"
	case PhasePathResolution:
		return "path resolution"
	case PhaseProviderDiscovery:
		return "provider discovery"
	case PhaseConnecting:
		return "connecting to providers"
	case PhaseDataRetrieval:
		return "data retrieval"
	default:
		return "unknown"
	}
}

// RetrievalState tracks diagnostic information about IPFS content retrieval operations.
// It is safe for concurrent use and maintains monotonic stage progression.
// Use ContextWithState to add tracking to a context, and StateFromContext
// to retrieve the state for updates or inspection
type RetrievalState struct {
	// ProvidersFound tracks the number of providers discovered for the content.
	ProvidersFound atomic.Int32
	// ProvidersAttempted tracks the number of providers we tried to connect to.
	ProvidersAttempted atomic.Int32
	// ProvidersConnected tracks the number of providers successfully connected.
	ProvidersConnected atomic.Int32

	// phase tracks the current retrieval phase (stored as int32)
	phase atomic.Int32

	// mu protects failedProviders slice during concurrent access
	mu sync.RWMutex
	// Track specific provider failures (limited to first few for brevity)
	failedProviders []peer.ID
}

// NewRetrievalState creates a new RetrievalState initialized to PhaseInitializing.
// The returned state is safe for concurrent use.
func NewRetrievalState() *RetrievalState {
	rs := &RetrievalState{}
	rs.phase.Store(int32(PhaseInitializing))
	return rs
}

// SetPhase updates the current retrieval phase to the given phase.
// The phase progression is monotonic - phases can only move forward, never backward.
// If the provided phase is less than or equal to the current phase, this is a no-op.
// This method is safe for concurrent use.
func (rs *RetrievalState) SetPhase(phase RetrievalPhase) {
	newPhase := int32(phase)
	for {
		current := rs.phase.Load()
		// Only update if the new phase is greater (moving forward)
		if newPhase <= current {
			return
		}
		// Try to update atomically
		if rs.phase.CompareAndSwap(current, newPhase) {
			return
		}
		// If CAS failed, another goroutine updated it, loop will check again
	}
}

// GetPhase returns the current retrieval phase.
// This method is safe for concurrent use.
func (rs *RetrievalState) GetPhase() RetrievalPhase {
	return RetrievalPhase(rs.phase.Load())
}

// AddFailedProvider records a provider peer ID that failed to deliver the requested content.
// Only the first MaxFailedProvidersToTrack providers are kept to prevent unbounded memory growth.
// This method is safe for concurrent use.
func (rs *RetrievalState) AddFailedProvider(peerID peer.ID) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if len(rs.failedProviders) < MaxFailedProvidersToTrack {
		rs.failedProviders = append(rs.failedProviders, peerID)
	}
}

// GetFailedProviders returns the list of failed providers
func (rs *RetrievalState) GetFailedProviders() []peer.ID {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return slices.Clone(rs.failedProviders)
}

// Summary generates a human-readable summary of the retrieval state,
// useful for timeout error messages and diagnostics.
func (rs *RetrievalState) Summary() string {
	found := rs.ProvidersFound.Load()
	attempted := rs.ProvidersAttempted.Load()
	connected := rs.ProvidersConnected.Load()
	phase := rs.GetPhase()

	if found == 0 {
		return fmt.Sprintf("no providers found for the CID (phase: %s)", phase.String())
	}

	if attempted == 0 {
		return fmt.Sprintf("found %d provider(s) but none could be contacted (phase: %s)", found, phase.String())
	}

	failedProviders := rs.GetFailedProviders()
	failedPeersInfo := ""
	if len(failedProviders) > 0 {
		// Convert peer IDs to strings for display
		peerStrings := make([]string, len(failedProviders))
		for i, p := range failedProviders {
			peerStrings[i] = p.String()
		}
		failedPeersInfo = fmt.Sprintf(", failed peers: %v", peerStrings)
	}

	if connected == 0 {
		return fmt.Sprintf("found %d provider(s), attempted %d, but none were reachable (phase: %s%s)",
			found, attempted, phase.String(), failedPeersInfo)
	}

	if len(failedProviders) > 0 {
		return fmt.Sprintf("found %d provider(s), connected to %d, but they did not return the requested content (phase: %s%s)",
			found, connected, phase.String(), failedPeersInfo)
	}

	return fmt.Sprintf("timeout occurred after finding %d provider(s) and connecting to %d (phase: %s)",
		found, connected, phase.String())
}

// ContextWithState ensures a RetrievalState exists in the context.
// If the context already contains a RetrievalState, it returns the existing one.
// Otherwise, it creates a new RetrievalState and adds it to the context.
// This function is idempotent and safe to call multiple times.
//
// Example:
//
//	ctx, retrievalState := retrieval.ContextWithState(ctx)
//	// Use retrievalState to track progress
//	retrievalState.SetStage(retrieval.StageProviderDiscovery)
func ContextWithState(ctx context.Context) (context.Context, *RetrievalState) {
	// Check if context already has a RetrievalState
	if existing := StateFromContext(ctx); existing != nil {
		return ctx, existing
	}
	// Create new one if not present
	rs := NewRetrievalState()
	return context.WithValue(ctx, ContextKey, rs), rs
}

// StateFromContext retrieves the RetrievalState from the context.
// Returns nil if no RetrievalState is present in the context.
// This function is typically used by subsystems to check if retrieval tracking
// is enabled and to update the state if it is.
//
// Example:
//
//	if retrievalState := retrieval.StateFromContext(ctx); retrievalState != nil {
//	    retrievalState.SetStage(retrieval.StageBlockRetrieval)
//	    retrievalState.ProvidersFound.Add(1)
//	}
func StateFromContext(ctx context.Context) *RetrievalState {
	if v := ctx.Value(ContextKey); v != nil {
		return v.(*RetrievalState)
	}
	return nil
}

// Compile-time assertions to ensure ErrorWithState implements the expected interfaces.
var (
	_ error                       = (*ErrorWithState)(nil)
	_ interface{ Unwrap() error } = (*ErrorWithState)(nil)
)

// ErrorWithState wraps an error with retrieval state information.
// It preserves the retrieval diagnostics for programmatic access while
// providing human-readable error messages.
//
// The zero value is not useful; use WrapWithState to create instances.
type ErrorWithState struct {
	// err is the underlying error being wrapped.
	err error
	// state contains the retrieval diagnostic information.
	state *RetrievalState
}

// Error returns the error message with retrieval diagnostics appended.
// Format: "original error: retrieval: diagnostic summary"
//
// If err is nil, returns a generic message. If state is nil, returns
// just the underlying error message.
func (e *ErrorWithState) Error() string {
	if e.err == nil {
		if e.state != nil {
			return fmt.Sprintf("retrieval error: %s", e.state.Summary())
		}
		return "retrieval error with no underlying cause"
	}
	if e.state != nil {
		return fmt.Sprintf("%s: retrieval: %s", e.err.Error(), e.state.Summary())
	}
	return e.err.Error()
}

// Unwrap returns the wrapped error, allowing errors.Is and errors.As to work
// with the underlying error.
func (e *ErrorWithState) Unwrap() error {
	return e.err
}

// Is reports whether target matches this error or its underlying error.
// This allows errors.Is(err, &ErrorWithState{}) to match any ErrorWithState.
func (e *ErrorWithState) Is(target error) bool {
	_, ok := target.(*ErrorWithState)
	return ok
}

// RetrievalState returns the retrieval state associated with this error.
// This allows callers to access detailed diagnostics for custom handling.
func (e *ErrorWithState) RetrievalState() *RetrievalState {
	return e.state
}

// WrapWithState wraps an error with retrieval state from the context.
// It returns an *ErrorWithState that preserves the state for custom handling.
//
// The error is ALWAYS wrapped if retrieval state exists in the context,
// because even "no providers found" is meaningful diagnostic information.
// If the error is already an *ErrorWithState, it returns it unchanged to
// avoid double-wrapping.
//
// Example usage in a gateway or IPFS implementation:
//
//	func fetchBlock(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
//	    block, err := blockService.GetBlock(ctx, cid)
//	    if err != nil {
//	        // Wrap error with retrieval diagnostics if available
//	        return nil, retrieval.WrapWithState(ctx, err)
//	    }
//	    return block, nil
//	}
//
// Callers can then extract the state for custom handling:
//
//	var errWithState *retrieval.ErrorWithState
//	if errors.As(err, &errWithState) {
//	    state := errWithState.RetrievalState()
//	    if state.ProvidersFound.Load() == 0 {
//	        // Handle "content not in network" case specially
//	    }
//	}
func WrapWithState(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	// Check if already wrapped
	var existingErr *ErrorWithState
	if errors.As(err, &existingErr) {
		return err
	}

	if state := StateFromContext(ctx); state != nil {
		// Always wrap if we have retrieval state - even "no providers" is meaningful
		return &ErrorWithState{
			err:   err,
			state: state,
		}
	}
	return err
}
