package index

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	carv1 "github.com/ipld/go-car"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"golang.org/x/exp/mmap"
)

// Generate generates index given car v1 payload using the given codec.
func Generate(car io.ReaderAt, codec Codec) (Index, error) {
	indexcls, ok := IndexAtlas[codec]
	if !ok {
		return nil, fmt.Errorf("unknown codec: %#v", codec)
	}

	header, err := carv1.ReadHeader(bufio.NewReader(internalio.NewOffsetReader(car, 0)))
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}
	offset, err := carv1.HeaderSize(header)
	if err != nil {
		return nil, err
	}

	idx := indexcls()

	records := make([]Record, 0)
	rdr := internalio.NewOffsetReader(car, int64(offset))
	for {
		thisItemIdx := rdr.Offset()
		l, err := binary.ReadUvarint(rdr)
		thisItemForNxt := rdr.Offset()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		c, _, err := internalio.ReadCid(car, thisItemForNxt)
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

// GenerateFromFile walks a car v1 file and generates an index of cid->byte offset, then
// stors it in a separate file at the given path with extension `.idx`.
func GenerateFromFile(path string, codec Codec) error {
	store, err := mmap.Open(path)
	if err != nil {
		return err
	}
	idx, err := Generate(store, codec)
	if err != nil {
		return err
	}
	return Save(idx, path)
}
