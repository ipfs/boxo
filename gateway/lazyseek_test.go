package gateway

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type badSeeker struct {
	io.ReadSeeker
}

var errBadSeek = fmt.Errorf("bad seeker")

func (bs badSeeker) Seek(offset int64, whence int) (int64, error) {
	off, err := bs.ReadSeeker.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}
	return off, errBadSeek
}

func TestLazySeekerError(t *testing.T) {
	underlyingBuffer := strings.NewReader("fubar")
	s := &lazySeeker{
		reader: badSeeker{underlyingBuffer},
		size:   underlyingBuffer.Size(),
	}
	off, err := s.Seek(0, io.SeekEnd)
	assert.Nil(t, err)
	assert.Equal(t, s.size, off, "expected to seek to the end")

	// shouldn't have actually seeked.
	b, err := io.ReadAll(s)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(b), "expected to read nothing")

	// shouldn't need to actually seek.
	off, err = s.Seek(0, io.SeekStart)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), off, "expected to seek to the start")

	b, err = io.ReadAll(s)
	assert.Nil(t, err)
	assert.Equal(t, "fubar", string(b), "expected to read string")

	// should fail the second time.
	off, err = s.Seek(0, io.SeekStart)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), off, "expected to seek to the start")

	// right here...
	b, err = io.ReadAll(s)
	assert.NotNil(t, err)
	assert.Equal(t, errBadSeek, err)
	assert.Equal(t, 0, len(b), "expected to read nothing")
}

func TestLazySeeker(t *testing.T) {
	underlyingBuffer := strings.NewReader("fubar")
	s := &lazySeeker{
		reader: underlyingBuffer,
		size:   underlyingBuffer.Size(),
	}
	expectByte := func(b byte) {
		t.Helper()
		var buf [1]byte
		n, err := io.ReadFull(s, buf[:])
		assert.Nil(t, err)
		assert.Equal(t, 1, n, "expected to read one byte, read %d", n)
		assert.Equal(t, b, buf[0])
	}
	expectSeek := func(whence int, off, expOff int64, expErr string) {
		t.Helper()
		n, err := s.Seek(off, whence)
		if expErr == "" {
			assert.Nil(t, err)
		} else {
			assert.EqualError(t, err, expErr)
		}
		assert.Equal(t, expOff, n)
	}

	expectSeek(io.SeekEnd, 0, s.size, "")
	b, err := io.ReadAll(s)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(b), "expected to read nothing")
	expectSeek(io.SeekEnd, -1, s.size-1, "")
	expectByte('r')
	expectSeek(io.SeekStart, 0, 0, "")
	expectByte('f')
	expectSeek(io.SeekCurrent, 1, 2, "")
	expectByte('b')
	expectSeek(io.SeekCurrent, -100, 3, "invalid seek offset")
}
