package car

import (
	"bufio"
	"fmt"
	"io"

	internalio "github.com/ipld/go-car/v2/internal/io"

	carv1 "github.com/ipld/go-car"
)

const version2 = 2

// Reader represents a reader of CAR v2.
type Reader struct {
	Header Header
	r      io.ReaderAt
}

// NewReader constructs a new reader that reads CAR v2 from the given r.
// Upon instantiation, the reader inspects the payload by reading the first 11 bytes and will return
// an error if the payload does not represent a CAR v2.
func NewReader(r io.ReaderAt) (*Reader, error) {
	cr := &Reader{
		r: r,
	}
	if err := cr.readPragma(); err != nil {
		return nil, err
	}
	if err := cr.readHeader(); err != nil {
		return nil, err
	}
	return cr, nil
}

func (r *Reader) readPragma() (err error) {
	pr := io.NewSectionReader(r.r, 0, PragmaSize)
	header, err := carv1.ReadHeader(bufio.NewReader(pr))
	if err != nil {
		return
	}
	if header.Version != version2 {
		err = fmt.Errorf("invalid car version: %d", header.Version)
	}
	return
}

func (r *Reader) readHeader() (err error) {
	headerSection := io.NewSectionReader(r.r, PragmaSize, HeaderSize)
	_, err = r.Header.ReadFrom(headerSection)
	return
}

// CarV1ReaderAt provides an io.ReaderAt containing the CAR v1 dump encapsulated in this CAR v2.
func (r *Reader) CarV1ReaderAt() io.ReaderAt {
	return io.NewSectionReader(r.r, int64(r.Header.CarV1Offset), int64(r.Header.CarV1Size))
}

// IndexReaderAt provides an io.ReaderAt containing the carbs.Index of this CAR v2.
func (r *Reader) IndexReaderAt() io.ReaderAt {
	return internalio.NewOffsetReader(r.r, int64(r.Header.IndexOffset))
}
