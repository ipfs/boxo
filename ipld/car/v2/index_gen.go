package car

import (
	"fmt"
	"io"
	"os"

	internalio "github.com/ipld/go-car/v2/internal/io"

	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"

	"github.com/multiformats/go-varint"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/internal/carv1"
)

// GenerateIndex generates index for a given car in v1 format.
// The index can be stored using index.Save into a file or serialized using index.WriteTo.
func GenerateIndex(v1r io.Reader) (index.Index, error) {
	reader := internalio.ToByteReadSeeker(v1r)
	header, err := carv1.ReadHeader(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}

	if header.Version != 1 {
		return nil, fmt.Errorf("expected version to be 1, got %v", header.Version)
	}

	idx, err := index.New(multicodec.CarIndexSorted)
	if err != nil {
		return nil, err
	}
	records := make([]index.Record, 0)

	// Record the start of each frame, with first frame starring from current position in the
	// reader, i.e. right after the header, since we have only read the header so far.
	var frameOffset int64

	// The Seek call below is equivalent to getting the reader.offset directly.
	// We get it through Seek to only depend on APIs of a typical io.Seeker.
	// This would also reduce refactoring in case the utility reader is moved.
	if frameOffset, err = reader.Seek(0, io.SeekCurrent); err != nil {
		return nil, err
	}

	for {
		// Read the frame's length.
		frameLen, err := varint.ReadUvarint(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Null padding; treat zero-length frames as an EOF.
		// They don't contain a CID nor block, so they're not useful.
		// TODO: Amend the CARv1 spec to explicitly allow this.
		if frameLen == 0 {
			break
		}

		// Read the CID.
		cidLen, c, err := cid.CidFromReader(reader)
		if err != nil {
			return nil, err
		}
		records = append(records, index.Record{Cid: c, Idx: uint64(frameOffset)})

		// Seek to the next frame by skipping the block.
		// The frame length includes the CID, so subtract it.
		remainingFrameLen := int64(frameLen) - int64(cidLen)
		if frameOffset, err = reader.Seek(remainingFrameLen, io.SeekCurrent); err != nil {
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
	// Seek to the beginning, since reading the version changes the reader's offset.
	if _, err := rs.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	switch version {
	case 1:
		// Simply generate the index, since there can't be a pre-existing one.
		return GenerateIndex(rs)
	case 2:
		// Read CAR v2 format
		v2r, err := NewReader(internalio.ToReaderAt(rs))
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
