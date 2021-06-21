package index

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cheggaaa/pb/v3"
	carv1 "github.com/ipld/go-car"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"golang.org/x/exp/mmap"
)

// GenerateIndex provides a low-level interface to create an index over a
// reader to a car stream.
func GenerateIndex(store io.ReaderAt, size int64, codec Codec) (Index, error) {
	indexcls, ok := IndexAtlas[codec]
	if !ok {
		return nil, fmt.Errorf("unknown codec: %#v", codec)
	}

	bar := pb.New64(size)
	bar.Set(pb.Bytes, true)
	bar.Set(pb.Terminal, true)

	bar.Start()

	header, err := carv1.ReadHeader(bufio.NewReader(internalio.NewOffsetReader(store, 0)))
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}
	offset, err := carv1.HeaderSize(header)
	if err != nil {
		return nil, err
	}
	bar.Add64(int64(offset))

	idx := indexcls()

	records := make([]Record, 0)
	rdr := internalio.NewOffsetReader(store, int64(offset))
	for {
		thisItemIdx := rdr.Offset()
		l, err := binary.ReadUvarint(rdr)
		bar.Add64(int64(l))
		thisItemForNxt := rdr.Offset()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		c, _, err := internalio.ReadCid(store, thisItemForNxt)
		if err != nil {
			return nil, err
		}
		records = append(records, Record{c, uint64(thisItemIdx)})
		rdr.SeekOffset(thisItemForNxt + int64(l))
	}

	if err := idx.Load(records); err != nil {
		return nil, err
	}

	bar.Finish()

	return idx, nil
}

// Generate walks a car file and generates an index of cid->byte offset in it.
func Generate(path string, codec Codec) error {
	store, err := mmap.Open(path)
	if err != nil {
		return err
	}
	idx, err := GenerateIndex(store, 0, codec)
	if err != nil {
		return err
	}
	return Save(idx, path)
}
