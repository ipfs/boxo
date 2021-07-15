package index

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	internalio "github.com/ipld/go-car/v2/internal/io"

	"github.com/multiformats/go-multicodec"

	"github.com/multiformats/go-varint"

	"github.com/ipfs/go-cid"
)

// Codec table is a first var-int in CAR indexes
const (
	indexHashed codec = 0x300000 + iota
	indexSingleSorted
	indexGobHashed
)

type (
	// codec is used as a multicodec identifier for CAR index files
	codec int

	// Record is a pre-processed record of a car item and location.
	Record struct {
		cid.Cid
		Idx uint64
	}

	// Index provides an interface for looking up byte offset of a given CID.
	Index interface {
		Codec() multicodec.Code
		Marshal(w io.Writer) error
		Unmarshal(r io.Reader) error
		Get(cid.Cid) (uint64, error)
		Load([]Record) error
	}
)

// New constructs a new index corresponding to the given CAR index codec.
func New(codec multicodec.Code) (Index, error) {
	switch codec {
	case multicodec.CarIndexSorted:
		return newSorted(), nil
	default:
		return nil, fmt.Errorf("unknwon index codec: %v", codec)
	}
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
	code, err := varint.ReadUvarint(internalio.ToByteReader(r))
	if err != nil {
		return nil, err
	}
	codec := multicodec.Code(code)
	idx, err := New(codec)
	if err != nil {
		return nil, err
	}
	if err := idx.Unmarshal(r); err != nil {
		return nil, err
	}
	return idx, nil
}
