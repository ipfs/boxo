package iter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLimit(t *testing.T) {
	for _, c := range []struct {
		name       string
		input      []int
		limit      int
		expResults []int
	}{
		{
			name:       "caps a longer iterator",
			input:      []int{1, 2, 3, 4, 5},
			limit:      3,
			expResults: []int{1, 2, 3},
		},
		{
			name:       "limit larger than input yields all",
			input:      []int{1, 2, 3},
			limit:      10,
			expResults: []int{1, 2, 3},
		},
		{
			name:       "limit equal to input yields all",
			input:      []int{1, 2, 3},
			limit:      3,
			expResults: []int{1, 2, 3},
		},
		{
			name:       "zero limit means unbounded",
			input:      []int{1, 2, 3},
			limit:      0,
			expResults: []int{1, 2, 3},
		},
		{
			name:       "negative limit means unbounded",
			input:      []int{1, 2, 3},
			limit:      -1,
			expResults: []int{1, 2, 3},
		},
		{
			name:       "empty input yields nothing",
			input:      []int{},
			limit:      5,
			expResults: nil,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			it := Limit[int](FromSlice(c.input), c.limit)
			var res []int
			for it.Next() {
				res = append(res, it.Val())
			}
			assert.Equal(t, c.expResults, res)
		})
	}
}

// closeTrackingIter records whether Close was called, to verify that
// LimitIter cascades Close to the wrapped iterator.
type closeTrackingIter[T any] struct {
	Iter[T]
	closed bool
}

func (c *closeTrackingIter[T]) Close() error {
	c.closed = true
	return c.Iter.Close()
}

func TestLimitClosesWrappedIter(t *testing.T) {
	inner := &closeTrackingIter[int]{Iter: FromSlice([]int{1, 2, 3})}
	it := Limit[int](inner, 2)

	require.NoError(t, it.Close())
	require.True(t, inner.closed, "Close must cascade to the wrapped iterator")
}
