package files

import (
	"io"
)

type FileEntry struct {
	File File
	Name string
}

// SliceFile implements File, and provides simple directory handling.
// It contains children files, and is created from a `[]File`.
// SliceFiles are always directories, and can't be read from or closed.
type SliceFile struct {
	files []FileEntry
	n     int
}

func NewSliceFile(files []FileEntry) Directory {
	return &SliceFile{files, 0}
}

func (f *SliceFile) NextFile() (string, File, error) {
	if f.n >= len(f.files) {
		return "", nil, io.EOF
	}
	file := f.files[f.n]
	f.n++
	return file.Name, file.File, nil
}

func (f *SliceFile) Close() error {
	return nil
}

func (f *SliceFile) Length() int {
	return len(f.files)
}

func (f *SliceFile) Size() (int64, error) {
	var size int64

	for _, file := range f.files {
		s, err := file.File.Size()
		if err != nil {
			return 0, err
		}
		size += s
	}

	return size, nil
}

var _ Directory = &SliceFile{}
