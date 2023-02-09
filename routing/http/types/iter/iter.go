package iter

import "io"

// Iter is an iterator of arbitrary values.
// Iterators are generally not goroutine-safe, to make them safe just read from them into a channel.
// For our use cases, these usually have a single reader. This motivates iterators instead of channels,
// since the overhead of goroutines+channels has a significant performance cost.
// Using an iterator, you can read results directly without necessarily involving the Go scheduler.
//
// There are a lot of options for an iterator interface, this one was picked for ease-of-use
// and for highest probability of consumers using it correctly.
// E.g. because there is a separate method for the value, it's easier to use in a loop but harder to implement.
//
// Hopefully in the future, Go will include an iterator in the language and we can remove this.
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

type IterCloser[T any] interface {
	Iter[T]
	io.Closer
}

type ResultIterCloser[T any] interface {
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

// iterCloserNoop creates an io.Closer from an Iter that does nothing on close.
type iterCloserNoop[T any] struct{ Iter[T] }

func (n *iterCloserNoop[T]) Close() error { return nil }

func IterCloserNoop[T any](it Iter[T]) IterCloser[T] {
	return &iterCloserNoop[T]{it}
}
