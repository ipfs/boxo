package files

import (
	"io"
	"os"
	"path/filepath"
)

// ReaderFile is a implementation of File created from an `io.Reader`.
// ReaderFiles are never directories, and can be read from and closed.
type ReaderFile struct {
	abspath string
	reader  io.ReadCloser
	stat    os.FileInfo
}

func NewReaderFile(reader io.ReadCloser, stat os.FileInfo) File {
	return &ReaderFile{"", reader, stat}
}

func NewReaderPathFile(path string, reader io.ReadCloser, stat os.FileInfo) (*ReaderFile, error) {
	abspath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	return &ReaderFile{abspath, reader, stat}, nil
}

func (f *ReaderFile) AbsPath() string {
	return f.abspath
}

func (f *ReaderFile) Read(p []byte) (int, error) {
	return f.reader.Read(p)
}

func (f *ReaderFile) Close() error {
	return f.reader.Close()
}

func (f *ReaderFile) Stat() os.FileInfo {
	return f.stat
}

func (f *ReaderFile) Size() (int64, error) {
	if f.stat == nil {
		return 0, ErrNotSupported
	}
	return f.stat.Size(), nil
}

func (f *ReaderFile) Seek(offset int64, whence int) (int64, error) {
	if s, ok := f.reader.(io.Seeker); ok {
		return s.Seek(offset, whence)
	}

	return 0, ErrNotSupported
}

var _ File = &ReaderFile{}
var _ FileInfo = &ReaderFile{}
