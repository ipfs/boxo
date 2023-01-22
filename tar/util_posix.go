//go:build linux || freebsd || netbsd || openbsd || dragonfly
// +build linux freebsd netbsd openbsd dragonfly

package tar

import (
	"golang.org/x/sys/unix"
	"syscall"
	"time"
	"unsafe"
)

func updateMtime(path string, mtime time.Time) error {
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

	return nil
}
