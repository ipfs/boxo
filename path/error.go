package path

import (
	"fmt"
)

type ErrInvalidPath struct {
	error error
	path  string
}

func (e ErrInvalidPath) Error() string {
	return fmt.Sprintf("invalid path %q: %s", e.path, e.error)
}

func (e ErrInvalidPath) Unwrap() error {
	return e.error
}

func (e ErrInvalidPath) Is(err error) bool {
	switch err.(type) {
	case ErrInvalidPath:
		return true
	default:
		return false
	}
}
