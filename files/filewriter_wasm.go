//go:build js && wasm
// +build js,wasm

package files

import (
	"errors"
	"os"
	"strings"
)

// In a WebAssembly context, direct file system operations like in Unix are not possible.
// Therefore, these functions could be adapted to work with in-memory storage or
// interfacing with JavaScript for file operations.

func createNewFile(path string) (*os.File, error) {
	// Since direct file creation isn't possible, you might want to simulate it or
	// interface with JavaScript. For now, this will return an error.
	return nil, errors.New("createNewFile is not supported in WebAssembly environments")
}

func isValidFilename(filename string) bool {
	// Filename validation might still be relevant in a WASM context.
	// You can keep the logic same or adapt it based on your application's needs.
	invalidChars := `/` + "\x00"
	return !strings.ContainsAny(filename, invalidChars)
}
