package index

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/ipfs/go-cid"
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

// Save writes a generated index into the given `path` as a file with a `.idx` extension.
func Save(i Index, path string) error {
	stream, err := os.OpenFile(path+".idx", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	defer stream.Close()
	return WriteTo(i, stream)
}

func WriteTo(i Index, w io.Writer) error {
	buf := make([]byte, binary.MaxVarintLen64)
	b := binary.PutUvarint(buf, uint64(i.Codec()))
	if _, err := w.Write(buf[:b]); err != nil {
		return err
	}
	return i.Marshal(w)
}

func ReadFrom(uar io.Reader) (Index, error) {
	reader := bufio.NewReader(uar)
	codec, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	idx, ok := IndexAtlas[Codec(codec)]
	if !ok {
		return nil, fmt.Errorf("unknown codec: %d", codec)
	}
	idxInst := idx()
	if err := idxInst.Unmarshal(reader); err != nil {
		return nil, err
	}
	return idxInst, nil
}
