package index

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"golang.org/x/exp/mmap"
)

// Codec table is a first var-int in carbs indexes
const (
	IndexHashed Codec = iota + 0x300000
	IndexSorted
	IndexSingleSorted
	IndexGobHashed
	IndexInsertion
)

type (
	// Codec is used as a multicodec identifier for carbs index files
	Codec int

	// IndexCls is a constructor for an index type
	IndexCls func() Index

	// Record is a pre-processed record of a car item and location.
	Record struct {
		cid.Cid
		Idx uint64
	}

	// Index provides an interface for figuring out where in the car a given cid begins
	Index interface {
		Codec() Codec
		Marshal(w io.Writer) error
		Unmarshal(r io.Reader) error
		Get(cid.Cid) (uint64, error)
		Load([]Record) error
	}
)

// IndexAtlas holds known index formats
var IndexAtlas = map[Codec]IndexCls{
	IndexHashed:       mkHashed,
	IndexSorted:       mkSorted,
	IndexSingleSorted: mkSingleSorted,
	IndexGobHashed:    mkGobHashed,
	IndexInsertion:    mkInsertion,
}

// Save writes a generated index for a car at `path`
func Save(i Index, path string) error {
	stream, err := os.OpenFile(path+".idx", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	defer stream.Close()

	buf := make([]byte, binary.MaxVarintLen64)
	b := binary.PutUvarint(buf, uint64(i.Codec()))
	if _, err := stream.Write(buf[:b]); err != nil {
		return err
	}
	return i.Marshal(stream)
}

// Restore loads an index from an on-disk representation.
func Restore(path string) (Index, error) {
	reader, err := mmap.Open(path + ".idx")
	if err != nil {
		return nil, err
	}

	defer reader.Close()
	uar := internalio.NewOffsetReader(reader, 0)
	codec, err := binary.ReadUvarint(uar)
	if err != nil {
		return nil, err
	}
	idx, ok := IndexAtlas[Codec(codec)]
	if !ok {
		return nil, fmt.Errorf("unknown codec: %d", codec)
	}
	idxInst := idx()
	if err := idxInst.Unmarshal(uar); err != nil {
		return nil, err
	}

	return idxInst, nil
}
