package files

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteTo(t *testing.T) {
	sf := NewMapDirectory(map[string]Node{
		"1": NewBytesFile([]byte("Some text!\n")),
		"2": NewBytesFile([]byte("beep")),
		"3": NewMapDirectory(nil),
		"4": NewBytesFile([]byte("boop")),
		"5": NewMapDirectory(map[string]Node{
			"a": NewBytesFile([]byte("foobar")),
		}),
	})
	tmppath, err := os.MkdirTemp("", "files-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmppath)

	path := filepath.Join(tmppath, "output")

	err = WriteTo(sf, path)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{
		".":                       "",
		"1":                       "Some text!\n",
		"2":                       "beep",
		"3":                       "",
		"4":                       "boop",
		"5":                       "",
		filepath.FromSlash("5/a"): "foobar",
	}
	err = filepath.Walk(path, func(cpath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rpath, err := filepath.Rel(path, cpath)
		if err != nil {
			return err
		}
		data, ok := expected[rpath]
		if !ok {
			return fmt.Errorf("expected something at %q", rpath)
		}
		delete(expected, rpath)

		if info.IsDir() {
			if data != "" {
				return fmt.Errorf("expected a directory at %q", rpath)
			}
		} else {
			actual, err := os.ReadFile(cpath)
			if err != nil {
				return err
			}
			if string(actual) != data {
				return fmt.Errorf("expected %q, got %q", data, string(actual))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(expected) > 0 {
		t.Fatalf("failed to find: %#v", expected)
	}
}

func TestDontAllowOverwrite(t *testing.T) {
	tmppath, err := os.MkdirTemp("", "files-test")
	assert.NoError(t, err)
	defer os.RemoveAll(tmppath)

	path := filepath.Join(tmppath, "output")

	// Check we can actually write to the output path before trying invalid entries
	// and leave an existing entry to test overwrite protection.
	assert.NoError(t, WriteTo(NewMapDirectory(map[string]Node{
		"exisiting-entry": NewBytesFile(nil),
	}), path))

	assert.Equal(t, ErrPathExistsOverwrite, WriteTo(NewBytesFile(nil), filepath.Join(path)))
	assert.Equal(t, ErrPathExistsOverwrite, WriteTo(NewBytesFile(nil), filepath.Join(path, "exisiting-entry")))
	// The directory in `path` has already been created so this should fail too:
	assert.Equal(t, ErrPathExistsOverwrite, WriteTo(NewMapDirectory(map[string]Node{
		"any-name": NewBytesFile(nil),
	}), filepath.Join(path)))
	os.RemoveAll(path)
}
