package files

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

type mockFileInfo struct {
	os.FileInfo
	name  string
	mode  os.FileMode
	mtime time.Time
	size  int64
}

func (m *mockFileInfo) Name() string {
	return m.name
}

func (m *mockFileInfo) Mode() os.FileMode {
	return m.mode
}

func (m *mockFileInfo) IsDir() bool {
	return m.mode.IsDir()
}

func (m *mockFileInfo) ModTime() time.Time {
	return m.mtime
}

func (m *mockFileInfo) Size() int64 {
	return m.size
}

func (m *mockFileInfo) Sys() any {
	return nil
}

var _ os.FileInfo = &mockFileInfo{}

func TestFileFilter(t *testing.T) {
	includeHidden := true
	filter, err := NewFilter("", nil, includeHidden)
	if err != nil {
		t.Errorf("failed to create filter with empty rules")
	}
	if filter.IncludeHidden != includeHidden {
		t.Errorf("new filter should include hidden files")
	}
	_, err = NewFilter("ignoreFileThatDoesNotExist", nil, false)
	if err == nil {
		t.Errorf("creating a filter without an invalid ignore file path should have failed")
	}
	tmppath := t.TempDir()
	ignoreFilePath := filepath.Join(tmppath, "ignoreFile")
	ignoreFileContents := []byte("a.txt")
	if err := os.WriteFile(ignoreFilePath, ignoreFileContents, 0o666); err != nil {
		t.Fatal(err)
	}
	filterWithIgnoreFile, err := NewFilter(ignoreFilePath, nil, false)
	if err != nil {
		t.Errorf("failed to create filter with ignore file")
	}
	if !filterWithIgnoreFile.ShouldExclude(&mockFileInfo{name: "a.txt"}) {
		t.Errorf("filter should've excluded expected file from ignoreFile: %s", "a.txt")
	}
}

func TestFileFilterDirectoryRules(t *testing.T) {
	dir := func(name string) *mockFileInfo {
		return &mockFileInfo{name: name, mode: os.ModeDir}
	}
	file := func(name string) *mockFileInfo {
		return &mockFileInfo{name: name}
	}

	for _, tc := range []struct {
		name    string
		rules   []string
		info    *mockFileInfo
		exclude bool
	}{
		// A "bar/" rule matches directories only. Before the trailing-slash
		// fix it matched nothing, so the directory leaked through.
		{"dir-only rule excludes directory", []string{"bar/"}, dir("bar"), true},
		{"dir-only rule keeps file", []string{"bar/"}, file("bar"), false},
		// A plain "bar" rule still matches both, as it did before.
		{"plain rule excludes directory", []string{"bar"}, dir("bar"), true},
		{"plain rule excludes file", []string{"bar"}, file("bar"), true},
		// Glob rules without a trailing slash keep matching directories.
		{"glob rule excludes directory", []string{"*.app"}, dir("foo.app"), true},
		{"unrelated directory is kept", []string{"bar/"}, dir("baz"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter, err := NewFilter("", tc.rules, true)
			if err != nil {
				t.Fatalf("failed to create filter: %v", err)
			}
			if got := filter.ShouldExclude(tc.info); got != tc.exclude {
				t.Errorf("ShouldExclude(%q) = %v, want %v", tc.info.name, got, tc.exclude)
			}
		})
	}
}
