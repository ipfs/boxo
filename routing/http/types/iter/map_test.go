package iter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {
	for _, c := range []struct {
		input      Iter[int]
		f          func(int) int
		expResults []int
	}{
		{
			input:      FromSlice([]int{1, 2, 3}),
			f:          func(i int) int { return i + 1 },
			expResults: []int{2, 3, 4},
		},
		{
			input:      FromSlice([]int{}),
			f:          func(i int) int { return i + 1 },
			expResults: nil,
		},
		{
			input:      FromSlice([]int{1}),
			f:          func(i int) int { return i + 1 },
			expResults: []int{2},
		},
	} {
		t.Run(fmt.Sprintf("%v", c.input), func(t *testing.T) {
			iter := Map(c.input, c.f)
			var res []int
			for iter.Next() {
				res = append(res, iter.Val())
			}
			assert.Equal(t, c.expResults, res)
		})
	}
}
