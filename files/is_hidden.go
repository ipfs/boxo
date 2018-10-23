// +build !windows

package files

import (
	"path/filepath"
	"strings"
)

func IsHidden(name string, f File) bool {
	fName := filepath.Base(name)

	if strings.HasPrefix(fName, ".") && len(fName) > 1 {
		return true
	}

	return false
}
