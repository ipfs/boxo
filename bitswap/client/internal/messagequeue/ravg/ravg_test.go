package ravg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRAvgInt(t *testing.T) {
	avg := New[int](10)
	require.Equal(t, 10, avg.Len())

	avg.Put(1)
	avg.Put(2)
	avg.Put(3)

	require.Equal(t, 2, avg.Mean())

	for i := 0; i < 10; i++ {
		for j := 1; j <= avg.Len(); j++ {
			avg.Put(j)
		}
	}
	require.Equal(t, 5, avg.Mean())

	require.Equal(t, 5.5, avg.FMean())
}

func TestRAvgFloat(t *testing.T) {
	avg := New[float64](10)
	require.Equal(t, 10, avg.Len())

	avg.Put(1)
	avg.Put(2)
	avg.Put(3)

	require.Equal(t, 2.0, avg.Mean())

	for i := 0; i < 10; i++ {
		for j := 1; j <= avg.Len(); j++ {
			avg.Put(float64(j))
		}
	}
	require.Equal(t, 5.5, avg.Mean())

	require.Equal(t, 5.5, avg.FMean())
}
