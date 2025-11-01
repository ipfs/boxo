//go:build windows

package tar

func invalidFileNames() []string {
	return []string{
		"foo.",      // Cannot end in '.'.
		"foo ",      // Cannot end in ' '.
		"..",        // Cannot be parent dir name.
		"CON",       // Cannot be device name.
		"nul",       // Cannot be device name (case insensitive).
		"AuX",       // Cannot be device name (case insensitive).
		"foo?",      // Cannot use reserved character '?'.
		`<\f|o:o*>`, // Cannot use reserved characters (multiple).
	}
}
