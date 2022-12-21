package iter

// Iter is an iterator of aribtrary values.
// Iterators are generally not goroutine-safe, to make them safe just read from them into a channel.
// For our use cases, these usually have a single reader. This motivates iterators instead of channels,
// since the overhead of goroutines+channels has a significant performance cost.
// Using an iterator, you can read results directly without necessarily involving the Go scheduler.
type Iter[T any] interface {
	// Next returns the next element, true if an attempt was made to get the next element, and any error that occurred.
	// You should generally check err before ok.
	Next() (val T, ok bool, err error)
	Close() error
}

func ReadAll[T any](iter Iter[T]) ([]T, error) {
	if iter == nil {
		return nil, nil
	}
	var vs []T
	for {
		v, ok, err := iter.Next()
		if err != nil {
			return vs, err
		}
		if !ok {
			return vs, nil
		}
		vs = append(vs, v)
	}
}
