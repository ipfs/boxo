package car

import "io"

// OffsetReader implements Read, and ReadAt on a section
// of an underlying io.ReaderAt.
// The main difference between io.SectionReader and OffsetReader is that
// NewOffsetReader accepts zero as n, the number of bytes to read, with
// the trade-off that it does not implement Seek.
// When n is set to zero, it will delegate io.EOF errors to the underlying
// io.ReaderAt.
// This is useful when reading a section at the end of a io.ReaderAt without
// having to know the total number readable of bytes.
type OffsetReader struct {
	r       io.ReaderAt
	base    int64
	off     int64
	limit   int64
	limited bool
}

// NewOffsetReader returns an OffsetReader that reads from r
// starting at offset off and stops with io.EOF after n bytes.
// If n is set to 0 then it will carry on reading until r reaches io.EOF.
func NewOffsetReader(r io.ReaderAt, off int64, n int64) *OffsetReader {
	return &OffsetReader{r, off, off, off + n, n == 0}
}

func (o *OffsetReader) Read(p []byte) (n int, err error) {
	if o.limited {
		if o.off >= o.limit {
			return 0, io.EOF
		}
		if max := o.limit - o.off; int64(len(p)) > max {
			p = p[0:max]
		}
	}
	n, err = o.r.ReadAt(p, o.off)
	o.off += int64(n)
	return
}

func (o *OffsetReader) ReadAt(p []byte, off int64) (n int, err error) {
	if o.limited {
		if off < 0 || off >= o.limit-o.base {
			return 0, io.EOF
		}
		off += o.base
		if max := o.limit - off; int64(len(p)) > max {
			p = p[0:max]
			n, err = o.r.ReadAt(p, off)
			if err == nil {
				err = io.EOF
			}
			return n, err
		}
	}
	return o.r.ReadAt(p, off)
}
