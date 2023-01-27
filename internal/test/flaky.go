package test

import (
	"os"
	"testing"
)

// Flaky will skip the test if the RUN_FLAKY_TESTS environment variable is empty.
func Flaky(t *testing.T) {
	// We can't use flags because it fails for tests that does not import this package
	if os.Getenv("RUN_FLAKY_TESTS") != "" {
		return
	}

	t.Skip("flaky")
}
