//go:build js && wasm
// +build js,wasm

package files

import (
	"errors"
	"os"
	"strings"
)

func createNewFile(path string) (*os.File, error) {
	return nil, errors.New("createNewFile is not supported in WebAssembly environments")
}

func isValidFilename(filename string) bool {
	// Filename validation might still be relevant in a WASM context.
	invalidChars := `/` + "\x00"
	return !strings.ContainsAny(filename, invalidChars)
}
