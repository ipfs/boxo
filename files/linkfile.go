package files

import (
	"io"
	"os"
	"strings"
)

type Symlink struct {
	path   string
	Target string
	stat   os.FileInfo

	reader io.Reader
}

func NewLinkFile(path, target string, stat os.FileInfo) File {
	return &Symlink{
		path:   path,
		Target: target,
		stat:   stat,
		reader: strings.NewReader(target),
	}
}

func (lf *Symlink) IsDirectory() bool {
	return false
}

func (lf *Symlink) NextFile() (string, File, error) {
	return "", nil, ErrNotDirectory
}

func (lf *Symlink) Close() error {
	if c, ok := lf.reader.(io.Closer); ok {
		return c.Close()
	}

	return nil
}

func (lf *Symlink) Read(b []byte) (int, error) {
	return lf.reader.Read(b)
}

func (lf *Symlink) Seek(offset int64, whence int) (int64, error) {
	if s, ok := lf.reader.(io.Seeker); ok {
		return s.Seek(offset, whence)
	}

	return 0, ErrNotSupported
}

func (lf *Symlink) Size() (int64, error) {
	return 0, ErrNotSupported
}
