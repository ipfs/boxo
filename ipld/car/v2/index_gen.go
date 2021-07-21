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
func GenerateIndex(v1r io.Reader, opts ...ReadOption) (index.Index, error) {
	var o ReadOptions
	for _, opt := range opts {
		opt(&o)
	}

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

	// Record the start of each section, with first section starring from current position in the
	// reader, i.e. right after the header, since we have only read the header so far.
	var sectionOffset int64

	// The Seek call below is equivalent to getting the reader.offset directly.
	// We get it through Seek to only depend on APIs of a typical io.Seeker.
	// This would also reduce refactoring in case the utility reader is moved.
	if sectionOffset, err = reader.Seek(0, io.SeekCurrent); err != nil {
		return nil, err
	}

	for {
		// Read the section's length.
		sectionLen, err := varint.ReadUvarint(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Null padding; by default it's an error.
		if sectionLen == 0 {
			if o.ZeroLengthSectionAsEOF {
				break
			} else {
				return nil, fmt.Errorf("carv1 null padding not allowed by default; see ZeroLengthSectionAsEOF")
			}
		}

		// Read the CID.
		cidLen, c, err := cid.CidFromReader(reader)
		if err != nil {
			return nil, err
		}
		records = append(records, index.Record{Cid: c, Offset: uint64(sectionOffset)})

		// Seek to the next section by skipping the block.
		// The section length includes the CID, so subtract it.
		remainingSectionLen := int64(sectionLen) - int64(cidLen)
		if sectionOffset, err = reader.Seek(remainingSectionLen, io.SeekCurrent); err != nil {
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

// ReadOrGenerateIndex accepts both CARv1 and CARv2 formats, and reads or generates an index for it.
// When the given reader is in CARv1 format an index is always generated.
// For a payload in CARv2 format, an index is only generated if Header.HasIndex returns false.
// An error is returned for all other formats, i.e. pragma with versions other than 1 or 2.
//
// Note, the returned index lives entirely in memory and will not depend on the
// given reader to fulfill index lookup.
func ReadOrGenerateIndex(rs io.ReadSeeker, opts ...ReadOption) (index.Index, error) {
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
		return GenerateIndex(rs, opts...)
	case 2:
		// Read CARv2 format
		v2r, err := NewReader(internalio.ToReaderAt(rs), opts...)
		if err != nil {
			return nil, err
		}
		// If index is present, then no need to generate; decode and return it.
		if v2r.Header.HasIndex() {
			return index.ReadFrom(v2r.IndexReader())
		}
		// Otherwise, generate index from CARv1 payload wrapped within CARv2 format.
		return GenerateIndex(v2r.DataReader(), opts...)
	default:
		return nil, fmt.Errorf("unknown version %v", version)
	}
}
