package index

import "errors"

var (
	// ErrNotFound signals a record is not found in the index.
	ErrNotFound = errors.New("not found")
	// errUnsupported signals unsupported operation by an index.
	errUnsupported = errors.New("not supported")
)
