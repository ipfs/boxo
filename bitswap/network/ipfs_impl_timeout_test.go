package network

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSendTimeout(t *testing.T) {
	require.Equal(t, minSendTimeout, sendTimeout(0))
	require.Equal(t, maxSendTimeout, sendTimeout(1<<30))

	// Check a 1MiB block (very large)
	oneMiB := uint64(1 << 20)
	hundredKbit := uint64(100 * 1000)
	hundredKB := hundredKbit / 8
	expectedTime := sendLatency + time.Duration(oneMiB*uint64(time.Second)/hundredKB)
	actualTime := sendTimeout(int(oneMiB))
	require.Equal(t, expectedTime, actualTime)

	// Check a 256KiB block (expected)
	require.InDelta(t, 25*time.Second, sendTimeout(256<<10), float64(5*time.Second))
}
