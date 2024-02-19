//go:build js && wasm
// +build js,wasm

package files

import (
    "testing"
    "github.com/stretchr/testify/assert"
)

func TestIsValidFilenameWASM(t *testing.T) {
    // Since file creation isn't supported in WASM, this test only focuses on filename validation.

    testCases := []struct {
        name     string
        valid    bool
    }{
        {"validfilename", true},
        {"/invalid/filename", false},
        {"", false},
        {".", false},
        {"..", false},
    }

    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            assert.Equal(t, tc.valid, isValidFilename(tc.name))
        })
    }
}
