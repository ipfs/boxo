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
