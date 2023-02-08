package iter

import "io"

// Map invokes f on each element of iter.
func Map[T any, U any](iter Iter[T], f func(t T) U) Iter[U] {
	return &mapIter[T, U]{iter: iter, f: f}
}

type mapIter[T any, U any] struct {
	iter Iter[T]
	f    func(T) U

	done bool
	val  U
}

func (m *mapIter[T, U]) Next() bool {
	if m.done {
		return false
	}

	ok := m.iter.Next()
	m.done = !ok

	if m.done {
		return false
	}

	m.val = m.f(m.iter.Val())

	return true
}

func (m *mapIter[T, U]) Val() U {
	return m.val
}

func (m *mapIter[T, U]) Close() error {
	if closer, ok := m.iter.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
