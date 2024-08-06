//go:build linux || freebsd || netbsd || openbsd || dragonfly
// +build linux freebsd netbsd openbsd dragonfly

package tar

import (
	"golang.org/x/sys/unix"
	"os"
	"syscall"
	"time"
	"unsafe"

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
		var AtFdCwd = -100
		pathname, err := syscall.BytePtrFromString(path)
		if err != nil {
			return err
		}

		tm := syscall.NsecToTimespec(mtime.UnixNano())
		ts := [2]syscall.Timespec{tm, tm}
		_, _, e := syscall.Syscall6(syscall.SYS_UTIMENSAT, uintptr(AtFdCwd),
			uintptr(unsafe.Pointer(pathname)), uintptr(unsafe.Pointer(&ts)),
			uintptr(unix.AT_SYMLINK_NOFOLLOW), 0, 0)
		if e != 0 {
			return error(e)
		}
	}
	return nil
}
