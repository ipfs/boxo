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

func (m *mockFileInfo) ModTime() time.Time {
	return m.mtime
}

func (m *mockFileInfo) Size() int64 {
	return m.size
}

func (m *mockFileInfo) Sys() interface{} {
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
