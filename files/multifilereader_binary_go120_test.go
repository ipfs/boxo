//go:build go1.20

package files

import (
	"testing"
)

func TestAbspathHeaderWithBinaryFilenameFails(t *testing.T) {
	// Simulates old client talking to new server (>= Go 1.20). Old client will
	// send the binary filename in the regular headers and the new server will error.
	runMultiFileReaderToMultiFileTest(t, true, true, true)
}
