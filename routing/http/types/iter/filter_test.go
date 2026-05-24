package iter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	for _, c := range []struct {
		input      Iter[int]
		f          func(int) bool
		expResults []int
	}{
		{
			input:      FromSlice([]int{1, 2, 3, 4}),
			f:          func(i int) bool { return i%2 == 0 },
			expResults: []int{2, 4},
		},
		{
			input:      FromSlice([]int{}),
			f:          func(i int) bool { return i%2 == 0 },
			expResults: nil,
		},
		{
			input:      FromSlice([]int{1, 3, 5, 100}),
			f:          func(i int) bool { return i > 2 },
			expResults: []int{3, 5, 100},
		},
		{
			input:      FromSlice([]int{2, 4, 6}),
			f:          func(i int) bool { return i%2 != 0 },
			expResults: nil,
		},
		{
			input:      FromSlice([]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 2}),
			f:          func(i int) bool { return i%2 == 0 },
			expResults: []int{2},
		},
	} {
		t.Run(fmt.Sprintf("%v", c.input), func(t *testing.T) {
			iter := Filter(c.input, c.f)
			var res []int
			for iter.Next() {
				res = append(res, iter.Val())
			}
			assert.Equal(t, c.expResults, res)
		})
	}
}

// TestFilterLongRejectRun exercises a long sequence of rejected values
// before a single match. The previous recursive implementation built one
// goroutine stack frame per rejected value, which post-PR-1157 (server
// pulls unbounded from the delegate) is reachable from any HTTP request
// that filters out most records.
func TestFilterLongRejectRun(t *testing.T) {
	const n = 10_000
	src := make([]int, n+1)
	src[n] = 1

	it := Filter(FromSlice(src), func(i int) bool { return i == 1 })

	require.True(t, it.Next())
	require.Equal(t, 1, it.Val())
	require.False(t, it.Next())
}
