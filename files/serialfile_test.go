package files

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func isFullPathHidden(p string) bool {
	return strings.HasPrefix(p, ".") || strings.Contains(p, "/.")
}

func TestSerialFile(t *testing.T) {
	t.Run("Hidden/NoFilter", func(t *testing.T) { testSerialFile(t, true, false) })
	t.Run("Hidden/Filter", func(t *testing.T) { testSerialFile(t, true, true) })
	t.Run("NotHidden/NoFilter", func(t *testing.T) { testSerialFile(t, false, false) })
	t.Run("NotHidden/Filter", func(t *testing.T) { testSerialFile(t, false, true) })
}

func testSerialFile(t *testing.T, hidden, withIgnoreRules bool) {
	tmppath, err := ioutil.TempDir("", "files-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmppath)

	testInputs := map[string]string{
		"1":      "Some text!\n",
		"2":      "beep",
		"3":      "",
		"4":      "boop",
		"5":      "",
		"5/a":    "foobar",
		".6":     "thing",
		"7":      "",
		"7/.foo": "bla",
		".8":     "",
		".8/foo": "bla",
	}
	fileFilter, err := NewFilter("", []string{"9", "10"}, hidden)
	if err != nil {
		t.Fatal(err)
	}
	if withIgnoreRules {
		testInputs["9"] = ""
		testInputs["9/b"] = "bebop"
		testInputs["10"] = ""
		testInputs["10/.c"] = "doowop"
	}

	for p, c := range testInputs {
		path := filepath.Join(tmppath, p)
		if c != "" {
			continue
		}
		if err := os.MkdirAll(path, 0777); err != nil {
			t.Fatal(err)
		}
	}

	for p, c := range testInputs {
		path := filepath.Join(tmppath, p)
		if c == "" {
			continue
		}
		if err := ioutil.WriteFile(path, []byte(c), 0666); err != nil {
			t.Fatal(err)
		}
	}
	expectedHiddenPaths := make([]string, 0, 4)
	expectedRegularPaths := make([]string, 0, 6)
	for p := range testInputs {
		path := filepath.Join(tmppath, p)
		stat, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !fileFilter.ShouldExclude(stat) {
			if isFullPathHidden(path) {
				expectedHiddenPaths = append(expectedHiddenPaths, p)
			} else {
				expectedRegularPaths = append(expectedRegularPaths, p)
			}
		}
	}

	stat, err := os.Stat(tmppath)
	if err != nil {
		t.Fatal(err)
	}

	sf, err := NewSerialFile(tmppath, hidden, stat)
	if withIgnoreRules {
		sf, err = NewSerialFileWithFilter(tmppath, fileFilter, stat)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()

	rootFound := false
	actualRegularPaths := make([]string, 0, len(expectedRegularPaths))
	actualHiddenPaths := make([]string, 0, len(expectedHiddenPaths))
	err = Walk(sf, func(path string, nd Node) error {
		defer nd.Close()

		// root node.
		if path == "" {
			if rootFound {
				return fmt.Errorf("found root twice")
			}
			if sf != nd {
				return fmt.Errorf("wrong root")
			}
			rootFound = true
			return nil
		}
		if isFullPathHidden(path) {
			actualHiddenPaths = append(actualHiddenPaths, path)
		} else {
			actualRegularPaths = append(actualRegularPaths, path)
		}
		if !hidden && isFullPathHidden(path) {
			return fmt.Errorf("found a hidden file")
		}
		if fileFilter.Rules.MatchesPath(path) {
			return fmt.Errorf("found a file that should be excluded")
		}

		data, ok := testInputs[path]
		if !ok {
			return fmt.Errorf("expected something at %q", path)
		}
		delete(testInputs, path)

		switch nd := nd.(type) {
		case *Symlink:
			return fmt.Errorf("didn't expect a symlink")
		case Directory:
			if data != "" {
				return fmt.Errorf("expected a directory at %q", path)
			}
		case File:
			actual, err := ioutil.ReadAll(nd)
			if err != nil {
				return err
			}
			if string(actual) != data {
				return fmt.Errorf("expected %q, got %q", data, string(actual))
			}
		}
		return nil
	})
	if !rootFound {
		t.Fatal("didn't find the root")
	}
	for _, regular := range expectedRegularPaths {
		if idx := sort.SearchStrings(actualRegularPaths, regular); idx < 0 {
			t.Errorf("missed regular path %q", regular)
		}
	}
	if hidden && len(actualHiddenPaths) != len(expectedHiddenPaths) {
		for _, missing := range expectedHiddenPaths {
			if idx := sort.SearchStrings(actualHiddenPaths, missing); idx < 0 {
				t.Errorf("missed hidden path %q", missing)
			}
		}
	}
}
