// +build windows

package files

import (
	"os"

	windows "golang.org/x/sys/windows"
)

func isHidden(fi os.FileInfo) bool {
	fName := fi.Name()
	switch fName {
	case "", ".", "..":
		return false
	}

	if fName[0] == '.' {
		return true
	}

	wi, ok := fi.Sys().(*windows.Win32FileAttributeData)
	if !ok {
		return false
	}

	return wi.FileAttributes&windows.FILE_ATTRIBUTE_HIDDEN != 0
}
