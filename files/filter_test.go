package files

import (
	"os"
	"path/filepath"
	"testing"
)

type mockFileInfo struct {
	os.FileInfo
	name string
}

func (m *mockFileInfo) Name() string {
	return m.name
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
	tmppath, err := os.MkdirTemp("", "filter-test")
	if err != nil {
		t.Fatal(err)
	}
	ignoreFilePath := filepath.Join(tmppath, "ignoreFile")
	ignoreFileContents := []byte("a.txt")
	if err := os.WriteFile(ignoreFilePath, ignoreFileContents, 0666); err != nil {
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
