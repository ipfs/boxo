package car

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyOptions_SetsExpectedDefaults(t *testing.T) {
	require.Equal(t, options{
		MaxTraversalLinks:     math.MaxInt64,
		TraverseLinksOnlyOnce: false,
	}, applyOptions())
}

func TestApplyOptions_AppliesOptions(t *testing.T) {
	require.Equal(t,
		options{
			MaxTraversalLinks:     123,
			TraverseLinksOnlyOnce: true,
		},
		applyOptions(
			MaxTraversalLinks(123),
			TraverseLinksOnlyOnce(),
		))
}
