package iter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	for _, c := range []struct {
		input      Iter[int]
		f          func(int) bool
		expResults []int
	}{
		{
			input:      FromSlice([]int{1, 2, 3}),
			f:          func(i int) bool { return i != 2 },
			expResults: []int{1, 3},
		},
		{
			input:      FromSlice([]int{}),
			f:          func(i int) bool { return true },
			expResults: nil,
		},
		{
			input:      FromSlice([]int{-3, -2, 1, -5, 2}),
			f:          func(i int) bool { return i > 0 },
			expResults: []int{1, 2},
		},
		{
			input:      FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9}),
			f:          func(i int) bool { return i%2 == 0 },
			expResults: []int{2, 4, 6, 8},
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
