// +build !windows

package tar

import (
	"fmt"
	"os"
	"strings"
)

func isNullDevice(path string) bool {
	return path == os.DevNull
}

func validatePlatformPath(platformPath string) error {
	if strings.Contains(platformPath, "\x00") {
		return fmt.Errorf("invalid platform path: path components cannot contain null: %q", platformPath)
	}
	return nil
}

func validatePathComponent(c string) error {
	if strings.Contains(c, "\x00") {
		return fmt.Errorf("invalid platform path: path components cannot contain null: %q", c)
	}
	return nil
}
