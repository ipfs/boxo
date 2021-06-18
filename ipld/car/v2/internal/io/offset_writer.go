package io

import "io"

var _ io.Writer = (*offsetWriter)(nil)

type offsetWriter struct {
	wa     io.WriterAt
	offset int64
}

func (ow *offsetWriter) Write(b []byte) (n int, err error) {
	n, err = ow.wa.WriteAt(b, ow.offset)
	ow.offset += int64(n)
	return
}
