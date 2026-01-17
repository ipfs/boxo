package files

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
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
	tmppath := t.TempDir()

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
		if err := os.MkdirAll(path, 0o777); err != nil {
			t.Fatal(err)
		}
	}

	for p, c := range testInputs {
		path := filepath.Join(tmppath, p)
		if c == "" {
			continue
		}
		if err := os.WriteFile(path, []byte(c), 0o666); err != nil {
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

	slices.Sort(expectedPaths)

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
				return errors.New("found root twice")
			}
			if sf != nd {
				return errors.New("wrong root")
			}
			rootFound = true
			return nil
		}
		actualPaths = append(actualPaths, path)
		if !hidden && isFullPathHidden(path) {
			return errors.New("found a hidden file")
		}
		components := filepath.SplitList(path)
		for i := range components {
			if fileFilter.Rules.MatchesPath(filepath.Join(components[:i+1]...)) {
				return errors.New("found a file that should be excluded")
			}
		}

		data, ok := testInputs[path]
		if !ok {
			return fmt.Errorf("expected something at %q", path)
		}
		delete(testInputs, path)

		switch nd := nd.(type) {
		case *Symlink:
			return errors.New("didn't expect a symlink")
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

func TestSerialFileSymlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping symlink test on Windows")
	}

	tmpdir := t.TempDir()

	// Create test structure:
	// tmpdir/
	//   realfile.txt     -> "real content"
	//   realdir/
	//     nested.txt     -> "nested content"
	//   linktofile       -> symlink to realfile.txt
	//   linktodir        -> symlink to realdir/

	realfile := filepath.Join(tmpdir, "realfile.txt")
	if err := os.WriteFile(realfile, []byte("real content"), 0o644); err != nil {
		t.Fatal(err)
	}

	realdir := filepath.Join(tmpdir, "realdir")
	if err := os.Mkdir(realdir, 0o755); err != nil {
		t.Fatal(err)
	}

	nested := filepath.Join(realdir, "nested.txt")
	if err := os.WriteFile(nested, []byte("nested content"), 0o644); err != nil {
		t.Fatal(err)
	}

	linktofile := filepath.Join(tmpdir, "linktofile")
	if err := os.Symlink(realfile, linktofile); err != nil {
		t.Fatal(err)
	}

	linktodir := filepath.Join(tmpdir, "linktodir")
	if err := os.Symlink(realdir, linktodir); err != nil {
		t.Fatal(err)
	}

	t.Run("preserve symlinks (default)", func(t *testing.T) {
		stat, err := os.Lstat(tmpdir)
		if err != nil {
			t.Fatal(err)
		}

		sf, err := NewSerialFileWithOptions(tmpdir, stat, SerialFileOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer sf.Close()

		symlinkCount := 0
		err = Walk(sf, func(path string, nd Node) error {
			defer nd.Close()
			if _, ok := nd.(*Symlink); ok {
				symlinkCount++
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if symlinkCount != 2 {
			t.Errorf("expected 2 symlinks, got %d", symlinkCount)
		}
	})

	t.Run("dereference symlinks", func(t *testing.T) {
		stat, err := os.Lstat(tmpdir)
		if err != nil {
			t.Fatal(err)
		}

		sf, err := NewSerialFileWithOptions(tmpdir, stat, SerialFileOptions{
			DereferenceSymlinks: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer sf.Close()

		symlinkCount := 0
		fileContents := make(map[string]string)
		dirCount := 0

		err = Walk(sf, func(path string, nd Node) error {
			defer nd.Close()
			switch n := nd.(type) {
			case *Symlink:
				symlinkCount++
			case File:
				data, err := io.ReadAll(n)
				if err != nil {
					return err
				}
				fileContents[path] = string(data)
			case Directory:
				if path != "" { // skip root
					dirCount++
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if symlinkCount != 0 {
			t.Errorf("expected 0 symlinks with dereferencing, got %d", symlinkCount)
		}

		// linktofile should now be a regular file with same content as realfile
		if content, ok := fileContents["linktofile"]; !ok {
			t.Error("linktofile not found as file")
		} else if content != "real content" {
			t.Errorf("linktofile content = %q, want %q", content, "real content")
		}

		// linktodir should now be a directory containing nested.txt
		if content, ok := fileContents["linktodir/nested.txt"]; !ok {
			t.Error("linktodir/nested.txt not found")
		} else if content != "nested content" {
			t.Errorf("linktodir/nested.txt content = %q, want %q", content, "nested content")
		}
	})

	t.Run("dereference single symlink to file", func(t *testing.T) {
		stat, err := os.Lstat(linktofile)
		if err != nil {
			t.Fatal(err)
		}

		sf, err := NewSerialFileWithOptions(linktofile, stat, SerialFileOptions{
			DereferenceSymlinks: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer sf.Close()

		// Should be a File, not a Symlink
		file, ok := sf.(File)
		if !ok {
			t.Fatalf("expected File, got %T", sf)
		}

		data, err := io.ReadAll(file)
		if err != nil {
			t.Fatal(err)
		}

		if string(data) != "real content" {
			t.Errorf("content = %q, want %q", string(data), "real content")
		}
	})

	t.Run("dereference single symlink to directory", func(t *testing.T) {
		stat, err := os.Lstat(linktodir)
		if err != nil {
			t.Fatal(err)
		}

		sf, err := NewSerialFileWithOptions(linktodir, stat, SerialFileOptions{
			DereferenceSymlinks: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer sf.Close()

		// Should be a Directory, not a Symlink
		dir, ok := sf.(Directory)
		if !ok {
			t.Fatalf("expected Directory, got %T", sf)
		}

		// Should contain nested.txt
		entries := dir.Entries()
		found := false
		for entries.Next() {
			if entries.Name() == "nested.txt" {
				found = true
				break
			}
		}
		if err := entries.Err(); err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Error("nested.txt not found in dereferenced directory")
		}
	})
}
