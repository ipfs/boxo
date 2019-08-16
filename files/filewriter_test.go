package files

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
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
	tmppath, err := ioutil.TempDir("", "files-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmppath)

	path := tmppath + "/output"

	err = WriteTo(sf, path)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{
		".":   "",
		"1":   "Some text!\n",
		"2":   "beep",
		"3":   "",
		"4":   "boop",
		"5":   "",
		"5/a": "foobar",
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
			actual, err := ioutil.ReadFile(cpath)
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
