package tar

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
var reservedNames = [...]string{"CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"}

const reservedCharsStr = `<>:"\|?*` + "\x00" // NOTE: `/` is not included as it is our standard path separator

func isNullDevice(path string) bool {
	// "NUL" is standard but the device is case insensitive.
	// Normalize to upper for the compare.
	const nameLen = len(os.DevNull)
	return len(path) == nameLen && strings.ToUpper(path) == os.DevNull
}

// validatePathComponent returns an error if the given path component is not allowed on the platform
func validatePathComponent(component string) error {
	const invalidPathErr = "invalid platform path"
	for _, suffix := range []string{
		".", // MSDN: Do not end a file or directory
		" ", // name with a space or a period
	} {
		if strings.HasSuffix(component, suffix) {
			return fmt.Errorf(
				`%s: path components cannot end with '%s': "%s"`,
				invalidPathErr, suffix, component,
			)
		}
	}
	if component == ".." {
		return fmt.Errorf(
			`%s: path component cannot be ".."`,
			invalidPathErr,
		)
	}
	if strings.ContainsAny(component, reservedCharsStr) {
		return fmt.Errorf(
			`%s: path components cannot contain any of "%s": "%s"`,
			invalidPathErr, reservedCharsStr, component,
		)
	}
	if slices.Contains(reservedNames[:], strings.ToUpper(component)) {
		return fmt.Errorf(
			`%s: path component is a reserved name: "%s"`,
			invalidPathErr, component,
		)
	}
	return nil
}

func validatePlatformPath(platformPath string) error {
	// remove the volume name
	p := platformPath[len(filepath.VolumeName(platformPath)):]

	// convert to cleaned slash-path
	p = filepath.ToSlash(p)
	p = strings.Trim(p, "/")

	// make sure all components of the path are valid
	for _, e := range strings.Split(p, "/") {
		if err := validatePathComponent(e); err != nil {
			return err
		}
	}
	return nil
}
