package iter

import "io"

// Iter is an iterator of arbitrary values.
// Iterators are generally not goroutine-safe, to make them safe just read from them into a channel.
// For our use cases, these usually have a single reader. This motivates iterators instead of channels,
// since the overhead of goroutines+channels has a significant performance cost.
// Using an iterator, you can read results directly without necessarily involving the Go scheduler.
type Iter[T any] interface {
	// Next sets the iterator to the next value, returning true if an attempt was made to get the next value.
	Next() bool
	Val() T
}

type ResultIter[T any] interface {
	Next() bool
	Val() Result[T]
}

type Result[T any] struct {
	Val T
	Err error
}

// ToResultIter returns an iterator that wraps each value in a Result.
func ToResultIter[T any](iter Iter[T]) Iter[Result[T]] {
	return Map(iter, func(t T) Result[T] {
		return Result[T]{Val: t}
	})
}

type ClosingIter[T any] interface {
	Iter[T]
	io.Closer
}

type ClosingResultIter[T any] interface {
	ResultIter[T]
	io.Closer
}

func ReadAll[T any](iter Iter[T]) []T {
	if iter == nil {
		return nil
	}
	var vs []T
	for iter.Next() {
		vs = append(vs, iter.Val())
	}
	return vs
}

type NoopClosingIter[T any] struct{ Iter[T] }

func (n *NoopClosingIter[T]) Close() error { return nil }
