package ravg

import (
	"golang.org/x/exp/constraints"
)

type Number interface {
	constraints.Integer | constraints.Float
}

type RAvg[T Number] struct {
	samples []T
	next    int
	full    bool
}

func New[T Number](size int) *RAvg[T] {
	return &RAvg[T]{
		samples: make([]T, size),
	}
}

func (r *RAvg[T]) Len() int {
	return len(r.samples)
}

func (r *RAvg[T]) Put(sample T) {
	r.samples[r.next] = sample
	r.next++
	if r.next == len(r.samples) {
		r.next = 0
		r.full = true
	}
}

func (r *RAvg[T]) Mean() T {
	size, sum := r.mean()
	return sum / T(size)
}

func (r *RAvg[T]) FMean() float64 {
	size, sum := r.mean()
	return float64(sum) / float64(size)
}

func (r *RAvg[T]) mean() (int, T) {
	size := len(r.samples)
	if !r.full {
		size = r.next
	}
	var sum T
	for i := 0; i < size; i++ {
		sum += r.samples[i]
	}
	return size, sum
}
