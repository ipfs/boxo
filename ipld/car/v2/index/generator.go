package index

import (
	"bufio"
	"fmt"
	"io"
	"os"

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

// Generate generates index for a given car in v1 format.
// The index can be stored using index.Save into a file or serialized using index.WriteTo.
func Generate(v1 io.ReadSeeker) (Index, error) {
	header, err := carv1.ReadHeader(bufio.NewReader(v1))
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}

	// TODO: ensure the input's header version is 1.

	offset, err := carv1.HeaderSize(header)
	if err != nil {
		return nil, err
	}

	idx := newSorted()

	records := make([]Record, 0)

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
		records = append(records, Record{c, uint64(frameOffset)})

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

// GenerateFromFile walks a car v1 file at the give path and generates an index of cid->byte offset.
// The index can be stored using index.Save into a file or serialized using index.WriteTo.
func GenerateFromFile(path string) (Index, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Generate(f)
}
