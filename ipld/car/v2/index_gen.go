package car

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"

	"github.com/multiformats/go-varint"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/internal/carv1"
)

type readSeekerPlusByte struct {
	io.ReadSeeker
}

func (r readSeekerPlusByte) ReadByte() (byte, error) {
	var p [1]byte
	_, err := io.ReadFull(r, p[:])
	return p[0], err
}

// GenerateIndex generates index for a given car in v1 format.
// The index can be stored using index.Save into a file or serialized using index.WriteTo.
func GenerateIndex(v1 io.ReadSeeker) (index.Index, error) {
	header, err := carv1.ReadHeader(v1)
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}

	if header.Version != 1 {
		return nil, fmt.Errorf("expected version to be 1, got %v", header.Version)
	}

	offset, err := carv1.HeaderSize(header)
	if err != nil {
		return nil, err
	}

	idx, err := index.New(multicodec.CarIndexSorted)
	if err != nil {
		return nil, err
	}
	records := make([]index.Record, 0)

	// Seek to the first frame.
	// Record the start of each frame, which we need for the index records.
	frameOffset := int64(0)
	if frameOffset, err = v1.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	for {
		// Grab the length of the frame.
		// Note that ReadUvarint wants a ByteReader.
		length, err := varint.ReadUvarint(readSeekerPlusByte{v1})
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Null padding; treat zero-length frames as an EOF.
		// They don't contain a CID nor block, so they're not useful.
		// TODO: Amend the CARv1 spec to explicitly allow this.
		if length == 0 {
			break
		}

		// Grab the CID.
		n, c, err := cid.CidFromReader(v1)
		if err != nil {
			return nil, err
		}
		records = append(records, index.Record{Cid: c, Idx: uint64(frameOffset)})

		// Seek to the next frame by skipping the block.
		// The frame length includes the CID, so subtract it.
		if frameOffset, err = v1.Seek(int64(length)-int64(n), io.SeekCurrent); err != nil {
			return nil, err
		}
	}

	if err := idx.Load(records); err != nil {
		return nil, err
	}

	return idx, nil
}

// GenerateIndexFromFile walks a car v1 file at the give path and generates an index of cid->byte offset.
// The index can be stored using index.Save into a file or serialized using index.WriteTo.
func GenerateIndexFromFile(path string) (index.Index, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return GenerateIndex(f)
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
		return GenerateIndex(rs)
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
		return GenerateIndex(v2r.CarV1Reader())
	default:
		return nil, fmt.Errorf("unknown version %v", version)
	}
}
