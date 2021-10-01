package car_test

import (
	"math"
	"testing"

	car "github.com/ipld/go-car"
	"github.com/stretchr/testify/require"
)

func TestApplyOptions_SetsExpectedDefaults(t *testing.T) {
	require.Equal(t, car.Options{
		MaxTraversalLinks:     math.MaxInt64,
		TraverseLinksOnlyOnce: false,
	}, car.ApplyOptions())
}

func TestApplyOptions_AppliesOptions(t *testing.T) {
	require.Equal(t,
		car.Options{
			MaxTraversalLinks:     123,
			TraverseLinksOnlyOnce: true,
		},
		car.ApplyOptions(
			car.MaxTraversalLinks(123),
			car.TraverseLinksOnlyOnce(),
		))
}
