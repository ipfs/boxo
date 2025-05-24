package httpnet

// Write tests for the errorTracker implementation found in watcher.go
import (
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestErrorTracker_StopTracking(t *testing.T) {
	et := newErrorTracker(&Network{})
	p := peer.ID("testpeer")

	// Stop tracking the peer
	et.stopTracking(p)

	// Check if the error count is removed
	if _, ok := et.errors[p]; ok {
		t.Errorf("Expected peer %s to be untracked but it was still tracked", p)
	}
}

func TestErrorTracker_LogErrors_Reset(t *testing.T) {
	et := newErrorTracker(&Network{})
	p := peer.ID("testpeer")

	// Log some errors
	err := et.logErrors(p, 5, 10)
	if err != nil {
		t.Errorf("Expected no error when logging errors but got %v", err)
	}

	// Reset error count
	err = et.logErrors(p, 0, 10)
	if err != nil {
		t.Errorf("Expected no error when resetting error count but got %v", err)
	}

	// Check if the error count is reset to 0
	count := et.errors[p]
	if count != 0 {
		t.Errorf("Expected error count for peer %s to be 0 after reset but got %d", p, count)
	}
}

func TestErrorTracker_LogErrors_ThresholdCrossed(t *testing.T) {
	et := newErrorTracker(&Network{})
	p := peer.ID("testpeer")

	// Log errors until threshold is crossed
	err := et.logErrors(p, 11, 10)
	if err != errThresholdCrossed {
		t.Errorf("Expected errorThresholdCrossed when logging errors above threshold but got %v", err)
	}

	// Check if the error count reflects the logged errors
	count, ok := et.errors[p]
	if !ok {
		t.Errorf("Expected peer %s to be tracked but it was not", p)
	}
	if count != 11 {
		t.Errorf("Expected error count for peer %s to be 10 after logging errors above threshold but got %d", p, count)
	}
}

// Write a test that tests concurrent access to the methods
func TestErrorTracker_ConcurrentAccess(t *testing.T) {
	et := newErrorTracker(&Network{})
	p := peer.ID("testpeer")

	var wg sync.WaitGroup
	numRoutines := 10
	threshold := 100

	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < int(threshold)/numRoutines; j++ {
				et.logErrors(p, 1, threshold)
			}
		}()
	}

	wg.Wait()

	// Check if the error count is correct
	count, ok := et.errors[p]
	if !ok {
		t.Errorf("Expected peer %s to be tracked but it was not", p)
	}
	expectedCount := threshold
	actualCount := count
	if actualCount != expectedCount {
		t.Errorf("Expected error count for peer %s to be %d after concurrent logging but got %d", p, expectedCount, actualCount)
	}
}
