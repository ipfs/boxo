package files

import (
	"fmt"
	"io"
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
	tmppath, err := os.MkdirTemp("", "files-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmppath)

	testInputs := map[string]string{
		"1":                          "Some text!\n",
		"2":                          "beep",
		"3":                          "",
		"4":                          "boop",
		"5":                          "",
		filepath.FromSlash("5/a"):    "foobar",
		".6":                         "thing",
		"7":                          "",
		filepath.FromSlash("7/.foo"): "bla",
		".8":                         "",
		filepath.FromSlash(".8/foo"): "bla",
	}
	fileFilter, err := NewFilter("", []string{"9", "10"}, hidden)
	if err != nil {
		t.Fatal(err)
	}
	if withIgnoreRules {
		testInputs["9"] = ""
		testInputs[filepath.FromSlash("9/b")] = "bebop"
		testInputs["10"] = ""
		testInputs[filepath.FromSlash("10/.c")] = "doowop"
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
		if err := os.WriteFile(path, []byte(c), 0666); err != nil {
			t.Fatal(err)
		}
	}
	expectedPaths := make([]string, 0, 4)
	expectedSize := int64(0)

testInputs:
	for p := range testInputs {
		components := strings.Split(p, string(filepath.Separator))
		var stat os.FileInfo
		for i := range components {
			stat, err = os.Stat(filepath.Join(
				append([]string{tmppath}, components[:i+1]...)...,
			))
			if err != nil {
				t.Fatal(err)
			}
			if fileFilter.ShouldExclude(stat) {
				continue testInputs
			}
		}
		expectedPaths = append(expectedPaths, p)
		if stat.Mode().IsRegular() {
			expectedSize += stat.Size()
		}
	}

	sort.Strings(expectedPaths)

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

	if size, err := sf.Size(); err != nil {
		t.Fatalf("failed to determine size: %s", err)
	} else if size != expectedSize {
		t.Fatalf("expected size %d, got size %d", expectedSize, size)
	}

	rootFound := false
	actualPaths := make([]string, 0, len(expectedPaths))
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
		actualPaths = append(actualPaths, path)
		if !hidden && isFullPathHidden(path) {
			return fmt.Errorf("found a hidden file")
		}
		components := filepath.SplitList(path)
		for i := range components {
			if fileFilter.Rules.MatchesPath(filepath.Join(components[:i+1]...)) {
				return fmt.Errorf("found a file that should be excluded")
			}
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
			actual, err := io.ReadAll(nd)
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
	if !rootFound {
		t.Fatal("didn't find the root")
	}

	if len(expectedPaths) != len(actualPaths) {
		t.Fatalf("expected %d paths, found %d",
			len(expectedPaths),
			len(actualPaths),
		)
	}

	for i := range expectedPaths {
		if expectedPaths[i] != actualPaths[i] {
			t.Errorf(
				"expected path %q does not match actual %q",
				expectedPaths[i],
				actualPaths[i],
			)
		}
	}
}
