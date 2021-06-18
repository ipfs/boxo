package index

import "errors"

var (
	// errNotFound is returned for lookups to entries that don't exist
	errNotFound    = errors.New("not found")
	errUnsupported = errors.New("not supported")
)
