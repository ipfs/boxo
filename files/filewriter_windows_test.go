//go:build windows

package files

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteToInvalidPaths(t *testing.T) {
	tmppath, err := os.MkdirTemp("", "files-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tmppath)

	path := filepath.Join(tmppath, "output")

	// Check we can actually write to the output path before trying invalid entries.
	assert.NoError(t, WriteTo(NewMapDirectory(map[string]Node{
		"valid-entry": NewBytesFile(nil),
	}), path))
	os.RemoveAll(path)

	// Now try all invalid entry names
	for _, entryName := range []string{"", ".", "..", "/", "", "not/a/base/path",
		"<", ">", ":", "\"", "\\", "|", "?", "*", "\x00",
		"CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"} {
		assert.Equal(t, ErrInvalidDirectoryEntry, WriteTo(NewMapDirectory(map[string]Node{
			entryName: NewBytesFile(nil),
		}), filepath.Join(path)))
		os.RemoveAll(path)
	}
}
