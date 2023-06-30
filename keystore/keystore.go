package keystore

import (
	"fmt"
)

// ErrNoSuchKey is an error message returned when no key of a given name was found.
var ErrNoSuchKey = fmt.Errorf("no key by the given name was found")

// ErrKeyExists is an error message returned when a key already exists
var ErrKeyExists = fmt.Errorf("key by that name already exists, refusing to overwrite")
