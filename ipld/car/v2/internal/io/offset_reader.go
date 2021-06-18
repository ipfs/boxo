package io

import "io"

var _ io.ReaderAt = (*OffsetReader)(nil)

// OffsetReader implements Read, and ReadAt on a section
// of an underlying io.ReaderAt.
// The main difference between io.SectionReader and OffsetReader is that
// NewOffsetReader does not require the user to know the number of readable bytes.
type OffsetReader struct {
	r    io.ReaderAt
	base int64
	off  int64
}

// NewOffsetReader returns an OffsetReader that reads from r
// starting offset offset off and stops with io.EOF when r reaches its end.
func NewOffsetReader(r io.ReaderAt, off int64) *OffsetReader {
	return &OffsetReader{r, off, off}
}

func (o *OffsetReader) Read(p []byte) (n int, err error) {
	n, err = o.r.ReadAt(p, o.off)
	o.off += int64(n)
	return
}

func (o *OffsetReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, io.EOF
	}
	off += o.base
	return o.r.ReadAt(p, off)
}

func (o *OffsetReader) ReadByte() (byte, error) {
	b := []byte{0}
	_, err := o.Read(b)
	return b[0], err
}

func (o *OffsetReader) Offset() int64 {
	return o.off
}

func (o *OffsetReader) SeekOffset(off int64) {
	o.off = off
}
