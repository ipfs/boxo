package car

import (
	"fmt"
	"io"

	internalio "github.com/ipld/go-car/v2/internal/io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/internal/carv1"
	"golang.org/x/exp/mmap"
)

// Reader represents a reader of CARv2.
type Reader struct {
	Header Header
	r      io.ReaderAt
	roots  []cid.Cid
	closer io.Closer
}

// OpenReader is a wrapper for NewReader which opens the file at path.
func OpenReader(path string, opts ...ReadOption) (*Reader, error) {
	f, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}

	r, err := NewReader(f, opts...)
	if err != nil {
		return nil, err
	}

	r.closer = f
	return r, nil
}

// NewReader constructs a new reader that reads CARv2 from the given r.
// Upon instantiation, the reader inspects the payload by reading the pragma and will return
// an error if the pragma does not represent a CARv2.
func NewReader(r io.ReaderAt, opts ...ReadOption) (*Reader, error) {
	cr := &Reader{
		r: r,
	}
	if err := cr.requireVersion2(); err != nil {
		return nil, err
	}
	if err := cr.readHeader(); err != nil {
		return nil, err
	}
	return cr, nil
}

func (r *Reader) requireVersion2() (err error) {
	or := internalio.NewOffsetReadSeeker(r.r, 0)
	version, err := ReadVersion(or)
	if err != nil {
		return
	}
	if version != 2 {
		return fmt.Errorf("invalid car version: %d", version)
	}
	return
}

// Roots returns the root CIDs.
// The root CIDs are extracted lazily from the data payload header.
func (r *Reader) Roots() ([]cid.Cid, error) {
	if r.roots != nil {
		return r.roots, nil
	}
	header, err := carv1.ReadHeader(r.DataReader())
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

// SectionReader implements both io.ReadSeeker and io.ReaderAt.
// It is the interface version of io.SectionReader, but note that the
// implementation is not guaranteed to be an io.SectionReader.
type SectionReader interface {
	io.Reader
	io.Seeker
	io.ReaderAt
}

// DataReader provides a reader containing the data payload in CARv1 format.
func (r *Reader) DataReader() SectionReader {
	return io.NewSectionReader(r.r, int64(r.Header.DataOffset), int64(r.Header.DataSize))
}

// IndexReader provides an io.Reader containing the index for the data payload.
func (r *Reader) IndexReader() io.Reader {
	return internalio.NewOffsetReadSeeker(r.r, int64(r.Header.IndexOffset))
}

// Close closes the underlying reader if it was opened by OpenReader.
func (r *Reader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

// ReadVersion reads the version from the pragma.
// This function accepts both CARv1 and CARv2 payloads.
func ReadVersion(r io.Reader) (version uint64, err error) {
	// TODO if the user provides a reader that sufficiently satisfies what carv1.ReadHeader is asking then use that instead of wrapping every time.
	header, err := carv1.ReadHeader(r)
	if err != nil {
		return
	}
	return header.Version, nil
}
