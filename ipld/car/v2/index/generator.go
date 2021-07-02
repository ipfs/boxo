package index

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/internal/carv1"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"golang.org/x/exp/mmap"
)

// Generate generates index for a given car in v1 format.
// The index can be stored using index.Save into a file or serialized using index.WriteTo.
func Generate(car io.ReaderAt) (Index, error) {
	header, err := carv1.ReadHeader(bufio.NewReader(internalio.NewOffsetReader(car, 0)))
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}

	// TODO: Generate should likely just take an io.ReadSeeker.
	// TODO: ensure the input's header version is 1.

	offset, err := carv1.HeaderSize(header)
	if err != nil {
		return nil, err
	}

	idx := mkSorted()

	records := make([]Record, 0)
	rdr := internalio.NewOffsetReader(car, int64(offset))
	for {
		thisItemIdx := rdr.Offset()
		l, err := binary.ReadUvarint(rdr)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		thisItemForNxt := rdr.Offset()
		_, c, err := cid.CidFromReader(rdr)
		if err != nil {
			return nil, err
		}
		records = append(records, Record{c, uint64(thisItemIdx)})
		rdr.SeekOffset(thisItemForNxt + int64(l))
	}

	if err := idx.Load(records); err != nil {
		return nil, err
	}

	return idx, nil
}

// GenerateFromFile walks a car v1 file at the give path and generates an index of cid->byte offset.
// The index can be stored using index.Save into a file or serialized using index.WriteTo.
func GenerateFromFile(path string) (Index, error) {
	store, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	return Generate(store)
}
