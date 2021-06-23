package car

import (
	"bufio"
	"fmt"
	"io"

	internalio "github.com/ipld/go-car/v2/internal/io"

	"github.com/ipfs/go-cid"
	carv1 "github.com/ipld/go-car"
)

// Reader represents a reader of CAR v2.
type Reader struct {
	Header Header
	r      io.ReaderAt
	roots  []cid.Cid
}

// NewReader constructs a new reader that reads CAR v2 from the given r.
// Upon instantiation, the reader inspects the payload by reading the pragma and will return
// an error if the pragma does not represent a CAR v2.
func NewReader(r io.ReaderAt) (*Reader, error) {
	cr := &Reader{
		r: r,
	}
	if err := cr.requireV2Pragma(); err != nil {
		return nil, err
	}
	if err := cr.readHeader(); err != nil {
		return nil, err
	}
	return cr, nil
}

func (r *Reader) requireV2Pragma() (err error) {
	or := internalio.NewOffsetReader(r.r, 0)
	version, _, err := ReadPragma(or)
	if err != nil {
		return
	}
	if version != 2 {
		return fmt.Errorf("invalid car version: %d", version)
	}
	if or.Offset() != PragmaSize {
		err = fmt.Errorf("invalid car v2 pragma; size %d is larger than expected %d", or.Offset(), PragmaSize)
	}
	return
}

// Roots returns the root CIDs of this CAR
func (r *Reader) Roots() ([]cid.Cid, error) {
	if r.roots != nil {
		return r.roots, nil
	}
	header, err := carv1.ReadHeader(bufio.NewReader(r.CarV1Reader()))
	if err != nil {
		return nil, err
	}
	r.roots = header.Roots
	return r.roots, nil
}

func (r *Reader) readHeader() (err error) {
	headerSection := io.NewSectionReader(r.r, PragmaSize, HeaderSize)
	_, err = r.Header.ReadFrom(headerSection)
	return
}

// CarV1Reader provides a reader containing the CAR v1 section encapsulated in this CAR v2.
func (r *Reader) CarV1Reader() *io.SectionReader {
	return io.NewSectionReader(r.r, int64(r.Header.CarV1Offset), int64(r.Header.CarV1Size))
}

// IndexReader provides an io.Reader containing the carbs.Index of this CAR v2.
func (r *Reader) IndexReader() io.Reader {
	return internalio.NewOffsetReader(r.r, int64(r.Header.IndexOffset))
}

// ReadPragma reads the pragma from r.
// This function accepts both CAR v1 and v2 payloads.
// The roots are returned only if the version of pragma equals 1, otherwise returns nil as roots.
func ReadPragma(r io.Reader) (version uint64, roots []cid.Cid, err error) {
	// TODO if the user provides a reader that sufficiently satisfies what carv1.ReadHeader is asking then use that instead of wrapping every time.
	header, err := carv1.ReadHeader(bufio.NewReader(r))
	if err != nil {
		return
	}
	version = header.Version
	if version == 1 {
		roots = header.Roots
	}
	return
}
