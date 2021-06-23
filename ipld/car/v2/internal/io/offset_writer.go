package io

import "io"

var _ io.Writer = (*OffsetWriter)(nil)

type OffsetWriter struct {
	w      io.WriterAt
	base   int64
	offset int64
}

func NewOffsetWriter(w io.WriterAt, off int64) *OffsetWriter {
	return &OffsetWriter{w, off, off}
}

func (ow *OffsetWriter) Write(b []byte) (n int, err error) {
	n, err = ow.w.WriteAt(b, ow.offset)
	ow.offset += int64(n)
	return
}

// Position returns the current position of this writer relative to the initial offset, i.e. the number of bytes written.
func (ow *OffsetWriter) Position() int64 {
	return ow.offset - ow.base
}
