package io

import "io"

func ToByteReader(r io.Reader) io.ByteReader {
	if br, ok := r.(io.ByteReader); ok {
		return br
	}
	return readerPlusByte{r}
}

type readerPlusByte struct {
	io.Reader
}

func (r readerPlusByte) ReadByte() (byte, error) {
	var p [1]byte
	_, err := io.ReadFull(r, p[:])
	return p[0], err
}
