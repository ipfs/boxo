package iter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// type nthErrIter[T any] struct {
// 	Iter[T]
// 	i   int
// 	n   int
// 	err error
// }

// func (n *nthErrIter[T]) Next() (T, bool) {
// 	v, ok := n.Iter.Next()
// 	n.i++
// 	return v, ok
// }
// func (n *nthErrIter[T]) Err() error {
// 	if n.i-1 == n.n {
// 		return n.err
// 	}
// 	return n.Iter.Err()
// }

// func nthErr[T any](iter Iter[T], n int, err error) Iter[T] {
// 	return &nthErrIter[T]{Iter: iter, n: n, err: err}
// }

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
		// {
		// 	input:      FromSlice([]int{}),
		// 	f:          func(i int) int { return i + 1 },
		// 	expResults: []int{},
		// },
		// {
		// 	input:      FromSlice([]int{1}),
		// 	f:          func(i int) int { return i + 1 },
		// 	expResults: []int{2},
		// },
		// {
		// 	input: FromSlice([]int{1, 2, 3}), 2, errors.New("boom"),
		// 	f: func(i int) (int, error) { return i + 1, nil },
		// 	expResults: []result{
		// 		{val: 2},
		// 		{val: 3},
		// 		{errContains: "boom"},
		// 	},
		// },
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
