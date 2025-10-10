//go:build !windows

package tar

func invalidFileNames() []string {
	return nil // No special cases for this platform.
}
