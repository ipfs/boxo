//go:build !linux && !freebsd && !netbsd && !openbsd && !dragonfly && !windows
// +build !linux,!freebsd,!netbsd,!openbsd,!dragonfly,!windows

package tar

import (
	"os"
	"time"

	"github.com/ipfs/boxo/files"
)

func updateMode(path string, mode int64) error {
	if mode != 0 {
		return os.Chmod(path, files.UnixPermsToModePerms(uint32(mode)))
	}
	return nil
}

func updateMtime(path string, mtime time.Time) error {
	if !mtime.IsZero() {
		return os.Chtimes(path, mtime, mtime)
	}
	return nil
}
