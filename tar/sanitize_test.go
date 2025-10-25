package tar

import (
	"testing"
)

func TestValidatePlatformPath(t *testing.T) {
	for _, name := range append(
		invalidFileNames(),
		"foo\x00bar", // Expect error if path contains null
	) {
		t.Run(name, func(t *testing.T) {
			if err := validatePlatformPath(name); err == nil {
				t.Fatal("expected error")
			}
		})
	}

	// No specification for a path component containing a "." component

	// Expect no error if path does not contain null
	if err := validatePlatformPath("foobar"); err != nil {
		t.Fatal(err)
	}
}

func TestValidatePathComponent(t *testing.T) {
	// Expect error if path is ".."
	if err := validatePathComponent(".."); err == nil {
		t.Fatal("expected error")
	}

	// Expect error if path contains null
	if err := validatePathComponent("foo\x00bar"); err == nil {
		t.Fatal("expected error")
	}

	// No specification for a path component that is "."

	// Expect no error if path does not contain null or ".."
	if err := validatePathComponent("foobar"); err != nil {
		t.Fatal(err)
	}
}
