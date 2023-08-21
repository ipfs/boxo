//go:build !go1.20

package files

import "testing"

func TestAbspathHeaderWithBinaryFilenameSucceeds(t *testing.T) {
	// Simulates old client talking to old server (< Go 1.20).
	runMultiFileReaderToMultiFileTest(t, true, true, false)
}
