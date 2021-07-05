package index

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/multiformats/go-varint"

	internalio "github.com/ipld/go-car/v2/internal/io"

	"github.com/ipfs/go-cid"
)

// Codec table is a first var-int in CAR indexes
const (
	IndexSorted Codec = 0x0400 // as per https://github.com/multiformats/multicodec/pull/220

	// TODO: unexport these before the final release, probably
	IndexHashed Codec = 0x300000 + iota
	IndexSingleSorted
	IndexGobHashed
	IndexInsertion
)

type (
	// Codec is used as a multicodec identifier for CAR index files
	// TODO: use go-multicodec before the final release
	Codec int

	// Builder is a constructor for an index type
	Builder func() Index

	// Record is a pre-processed record of a car item and location.
	Record struct {
		cid.Cid
		Idx uint64
	}

	// Index provides an interface for looking up byte offset of a given CID.
	Index interface {
		Codec() Codec
		Marshal(w io.Writer) error
		Unmarshal(r io.Reader) error
		Get(cid.Cid) (uint64, error)
		Load([]Record) error
	}
)

// BuildersByCodec holds known index formats
// TODO: turn this into a func before the final release?
var BuildersByCodec = map[Codec]Builder{
	IndexHashed:       mkHashed,
	IndexSorted:       mkSorted,
	IndexSingleSorted: mkSingleSorted,
	IndexGobHashed:    mkGobHashed,
	IndexInsertion:    mkInsertion,
}

// Save writes a generated index into the given `path`.
func Save(idx Index, path string) error {
	stream, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	defer stream.Close()
	return WriteTo(idx, stream)
}

// Attach attaches a given index to an existing car v2 file at given path and offset.
func Attach(path string, idx Index, offset uint64) error {
	out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	defer out.Close()
	indexWriter := internalio.NewOffsetWriter(out, int64(offset))
	return WriteTo(idx, indexWriter)
}

// WriteTo writes the given idx into w.
// The written bytes include the index encoding.
// This can then be read back using index.ReadFrom
func WriteTo(idx Index, w io.Writer) error {
	buf := make([]byte, binary.MaxVarintLen64)
	b := varint.PutUvarint(buf, uint64(idx.Codec()))
	if _, err := w.Write(buf[:b]); err != nil {
		return err
	}
	return idx.Marshal(w)
}

// ReadFrom reads index from r.
// The reader decodes the index by reading the first byte to interpret the encoding.
// Returns error if the encoding is not known.
func ReadFrom(r io.Reader) (Index, error) {
	reader := bufio.NewReader(r)
	codec, err := varint.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	builder, ok := BuildersByCodec[Codec(codec)]
	if !ok {
		return nil, fmt.Errorf("unknown codec: %d", codec)
	}
	idx := builder()
	if err := idx.Unmarshal(reader); err != nil {
		return nil, err
	}
	return idx, nil
}
