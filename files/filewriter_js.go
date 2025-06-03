//go:build js

package files

import (
	"os"
	"strings"
)

var invalidChars = `/` + "\x00"

func isValidFilename(filename string) bool {
	return !strings.ContainsAny(filename, invalidChars)
}

func createNewFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0o666)
}
