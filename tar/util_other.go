//go:build !linux && !freebsd && !netbsd && !openbsd && !dragonfly
// +build !linux,!freebsd,!netbsd,!openbsd,!dragonfly

package tar

import (
	"os"
	"time"
)

func updateMtime(path string, mtime time.Time) error {
	return os.Chtimes(path, mtime, mtime)
}
