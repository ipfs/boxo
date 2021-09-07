package io

import (
	"io"
	"io/ioutil"
	"sync"
)

var (
	_ io.ByteReader = (*readerPlusByte)(nil)
	_ io.ByteReader = (*readSeekerPlusByte)(nil)
	_ io.ByteReader = (*discardingReadSeekerPlusByte)(nil)
	_ io.ReadSeeker = (*discardingReadSeekerPlusByte)(nil)
	_ io.ReaderAt   = (*readSeekerAt)(nil)
)

type (
	readerPlusByte struct {
		io.Reader
	}

	readSeekerPlusByte struct {
		io.ReadSeeker
	}

	discardingReadSeekerPlusByte struct {
		io.Reader
		offset int64
	}

	ByteReadSeeker interface {
		io.ReadSeeker
		io.ByteReader
	}

	readSeekerAt struct {
		rs io.ReadSeeker
		mu sync.Mutex
	}
)

func ToByteReader(r io.Reader) io.ByteReader {
	if br, ok := r.(io.ByteReader); ok {
		return br
	}
	return &readerPlusByte{r}
}

func ToByteReadSeeker(r io.Reader) ByteReadSeeker {
	if brs, ok := r.(ByteReadSeeker); ok {
		return brs
	}
	if rs, ok := r.(io.ReadSeeker); ok {
		return &readSeekerPlusByte{rs}
	}
	return &discardingReadSeekerPlusByte{Reader: r}
}

func ToReaderAt(rs io.ReadSeeker) io.ReaderAt {
	if ra, ok := rs.(io.ReaderAt); ok {
		return ra
	}
	return &readSeekerAt{rs: rs}
}

func (rb *readerPlusByte) ReadByte() (byte, error) {
	return readByte(rb)
}

func (rsb *readSeekerPlusByte) ReadByte() (byte, error) {
	return readByte(rsb)
}

func (drsb *discardingReadSeekerPlusByte) ReadByte() (byte, error) {
	return readByte(drsb)
}

func (drsb *discardingReadSeekerPlusByte) Read(p []byte) (read int, err error) {
	read, err = drsb.Reader.Read(p)
	drsb.offset += int64(read)
	return
}

func (drsb *discardingReadSeekerPlusByte) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		n := offset - drsb.offset
		if n < 0 {
			panic("unsupported rewind via whence: io.SeekStart")
		}
		_, err := io.CopyN(ioutil.Discard, drsb, n)
		return drsb.offset, err
	case io.SeekCurrent:
		_, err := io.CopyN(ioutil.Discard, drsb, offset)
		return drsb.offset, err
	default:
		panic("unsupported whence: io.SeekEnd")
	}
}

func readByte(r io.Reader) (byte, error) {
	var p [1]byte
	_, err := io.ReadFull(r, p[:])
	return p[0], err
}

func (rsa *readSeekerAt) ReadAt(p []byte, off int64) (n int, err error) {
	rsa.mu.Lock()
	defer rsa.mu.Unlock()
	if _, err := rsa.rs.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	return rsa.rs.Read(p)
}
