package index

import "errors"

var (
	// errNotFound signals a record is not found in the index.
	errNotFound = errors.New("not found")
	// errUnsupported signals unsupported operation by an index.
	errUnsupported = errors.New("not supported")
)
