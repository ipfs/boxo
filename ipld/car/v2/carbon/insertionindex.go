package carbon

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/petar/GoLLRB/llrb"
	cbor "github.com/whyrusleeping/cbor/go"
	carbs "github.com/willscott/carbs"
)

// IndexInsertion carbs IndexCodec identifier
const IndexInsertion carbs.IndexCodec = 0x300010

// init hook to register the index format
func init() {
	carbs.IndexAtlas[IndexInsertion] = mkInsertion
}

type insertionIndex struct {
	items llrb.LLRB
}

type record struct {
	digest []byte
	carbs.Record
}

func (r record) Less(than llrb.Item) bool {
	other, ok := than.(record)
	if !ok {
		return false
	}
	return bytes.Compare(r.digest, other.digest) <= 0
}

func mkRecord(r carbs.Record) record {
	d, err := multihash.Decode(r.Hash())
	if err != nil {
		return record{}
	}

	return record{d.Digest, r}
}

func mkRecordFromCid(c cid.Cid, at uint64) record {
	d, err := multihash.Decode(c.Hash())
	if err != nil {
		return record{}
	}

	return record{d.Digest, carbs.Record{Cid: c, Idx: at}}
}

func (ii *insertionIndex) Get(c cid.Cid) (uint64, error) {
	d, err := multihash.Decode(c.Hash())
	if err != nil {
		return 0, err
	}
	entry := record{digest: d.Digest}
	e := ii.items.Get(entry)
	if e == nil {
		return 0, errNotFound
	}
	r, ok := e.(record)
	if !ok {
		return 0, errUnsupported
	}

	return r.Record.Idx, nil
}

func (ii *insertionIndex) Marshal(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, int64(ii.items.Len())); err != nil {
		return err
	}

	var err error

	iter := func(i llrb.Item) bool {
		if err = cbor.Encode(w, i.(record).Record); err != nil {
			return false
		}
		return true
	}
	ii.items.AscendGreaterOrEqual(ii.items.Min(), iter)
	return err
}

func (ii *insertionIndex) Unmarshal(r io.Reader) error {
	var len int64
	if err := binary.Read(r, binary.LittleEndian, &len); err != nil {
		return err
	}
	d := cbor.NewDecoder(r)
	for i := int64(0); i < len; i++ {
		var rec carbs.Record
		if err := d.Decode(&rec); err != nil {
			return err
		}
		ii.items.InsertNoReplace(mkRecord(rec))
	}
	return nil
}

// Codec identifies this index format
func (ii *insertionIndex) Codec() carbs.IndexCodec {
	return IndexInsertion
}

func (ii *insertionIndex) Load(rs []carbs.Record) error {
	for _, r := range rs {
		rec := mkRecord(r)
		if rec.digest == nil {
			return fmt.Errorf("invalid entry: %v", r)
		}
		ii.items.InsertNoReplace(rec)
	}
	return nil
}

func mkInsertion() carbs.Index {
	ii := insertionIndex{}
	return &ii
}

// Flatten returns a 'indexsorted' formatted index for more efficient subsequent loading
func (ii *insertionIndex) Flatten() (carbs.Index, error) {
	si := carbs.IndexAtlas[carbs.IndexSorted]()
	rcrds := make([]carbs.Record, ii.items.Len())

	idx := 0
	iter := func(i llrb.Item) bool {
		rcrds[idx] = i.(record).Record
		idx++
		return true
	}
	ii.items.AscendGreaterOrEqual(ii.items.Min(), iter)

	if err := si.Load(rcrds); err != nil {
		return nil, err
	}
	return si, nil
}
