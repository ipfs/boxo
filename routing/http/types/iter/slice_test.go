package iter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSliceIter(t *testing.T) {
	for _, c := range []struct {
		slice    []int
		expSlice []int
	}{
		{
			slice:    []int{1, 2, 3},
			expSlice: []int{1, 2, 3},
		},
		{
			slice:    nil,
			expSlice: nil,
		},
		{
			slice:    []int{},
			expSlice: nil,
		},
	} {
		t.Run(fmt.Sprintf("%+v", c.slice), func(t *testing.T) {
			iter := FromSlice(c.slice)
			var vals []int
			for iter.Next() {
				vals = append(vals, iter.Val())
			}
			require.Equal(t, c.expSlice, vals)
		})
	}
}
