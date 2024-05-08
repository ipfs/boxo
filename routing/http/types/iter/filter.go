package iter

// Filter invokes f on each element of iter, filtering the results.
func Filter[T any](iter Iter[T], f func(t T) bool) *FilterIter[T] {
	return &FilterIter[T]{iter: iter, f: f}
}

type FilterIter[T any] struct {
	iter Iter[T]
	f    func(T) bool

	done bool
	val  T
}

func (f *FilterIter[T]) Next() bool {
	if f.done {
		return false
	}

	ok := f.iter.Next()
	if !ok {
		f.done = true
		return false
	}

	val := f.iter.Val()
	if !f.f(val) {
		return f.Next()
	}

	f.val = val
	return true
}

func (f *FilterIter[T]) Val() T {
	return f.val
}

func (f *FilterIter[T]) Close() error {
	return f.iter.Close()
}
