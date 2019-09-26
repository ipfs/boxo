package files

import (
	"strings"
)

type Symlink struct {
	Target string

	reader strings.Reader
}

func NewLinkFile(target string) File {
	lf := &Symlink{Target: target}
	lf.reader.Reset(lf.Target)
	return lf
}

func (lf *Symlink) Close() error {
	return nil
}

func (lf *Symlink) Read(b []byte) (int, error) {
	return lf.reader.Read(b)
}

func (lf *Symlink) Seek(offset int64, whence int) (int64, error) {
	return lf.reader.Seek(offset, whence)
}

func (lf *Symlink) Size() (int64, error) {
	return lf.reader.Size(), nil
}

func ToSymlink(n Node) *Symlink {
	l, _ := n.(*Symlink)
	return l
}

var _ File = &Symlink{}
