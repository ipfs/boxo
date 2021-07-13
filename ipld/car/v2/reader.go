package car

import (
	"bufio"
	"fmt"
	"io"
	"sync"

	"github.com/ipld/go-car/v2/index"

	internalio "github.com/ipld/go-car/v2/internal/io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/internal/carv1"
	"golang.org/x/exp/mmap"
)

// Reader represents a reader of CAR v2.
type Reader struct {
	Header      Header
	r           io.ReaderAt
	roots       []cid.Cid
	carv2Closer io.Closer
}

// NewReaderMmap is a wrapper for NewReader which opens the file at path with
// x/exp/mmap.
func NewReaderMmap(path string) (*Reader, error) {
	f, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}

	r, err := NewReader(f)
	if err != nil {
		return nil, err
	}

	r.carv2Closer = f
	return r, nil
}

// NewReader constructs a new reader that reads CAR v2 from the given r.
// Upon instantiation, the reader inspects the payload by reading the pragma and will return
// an error if the pragma does not represent a CAR v2.
func NewReader(r io.ReaderAt) (*Reader, error) {
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

// SectionReader implements both io.ReadSeeker and io.ReaderAt.
// It is the interface version of io.SectionReader, but note that the
// implementation is not guaranteed to be an io.SectionReader.
type SectionReader interface {
	io.Reader
	io.Seeker
	io.ReaderAt
}

// CarV1Reader provides a reader containing the CAR v1 section encapsulated in this CAR v2.
func (r *Reader) CarV1Reader() SectionReader {
	return io.NewSectionReader(r.r, int64(r.Header.CarV1Offset), int64(r.Header.CarV1Size))
}

// IndexReader provides an io.Reader containing the index of this CAR v2.
func (r *Reader) IndexReader() io.Reader {
	return internalio.NewOffsetReadSeeker(r.r, int64(r.Header.IndexOffset))
}

// Close closes the underlying reader if it was opened by NewReaderMmap.
func (r *Reader) Close() error {
	if r.carv2Closer != nil {
		return r.carv2Closer.Close()
	}
	return nil
}

// ReadVersion reads the version from the pragma.
// This function accepts both CAR v1 and v2 payloads.
func ReadVersion(r io.Reader) (version uint64, err error) {
	// TODO if the user provides a reader that sufficiently satisfies what carv1.ReadHeader is asking then use that instead of wrapping every time.
	header, err := carv1.ReadHeader(bufio.NewReader(r))
	if err != nil {
		return
	}
	return header.Version, nil
}

var _ io.ReaderAt = (*readSeekerAt)(nil)

type readSeekerAt struct {
	rs io.ReadSeeker
	mu sync.Mutex
}

func (rsa *readSeekerAt) ReadAt(p []byte, off int64) (n int, err error) {
	rsa.mu.Lock()
	defer rsa.mu.Unlock()
	if _, err := rsa.rs.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	return rsa.rs.Read(p)
}

// ReadOrGenerateIndex accepts both CAR v1 and v2 format, and reads or generates an index for it.
// When the given reader is in CAR v1 format an index is always generated.
// For a payload in CAR v2 format, an index is only generated if Header.HasIndex returns false.
// An error is returned for all other formats, i.e. versions other than 1 or 2.
//
// Note, the returned index lives entirely in memory and will not depend on the
// given reader to fulfill index lookup.
func ReadOrGenerateIndex(rs io.ReadSeeker) (index.Index, error) {
	// Read version.
	version, err := ReadVersion(rs)
	if err != nil {
		return nil, err
	}
	// Seek to the begining, since reading the version changes the reader's offset.
	if _, err := rs.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	switch version {
	case 1:
		// Simply generate the index, since there can't be a pre-existing one.
		return index.Generate(rs)
	case 2:
		// Read CAR v2 format
		v2r, err := NewReader(&readSeekerAt{rs: rs})
		if err != nil {
			return nil, err
		}
		// If index is present, then no need to generate; decode and return it.
		if v2r.Header.HasIndex() {
			return index.ReadFrom(v2r.IndexReader())
		}
		// Otherwise, generate index from CAR v1 payload wrapped within CAR v2 format.
		return index.Generate(v2r.CarV1Reader())
	default:
		return nil, fmt.Errorf("unknown version %v", version)
	}
}
