package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/petar/GoLLRB/llrb"
	cbor "github.com/whyrusleeping/cbor/go"
)

type InsertionIndex struct {
	items llrb.LLRB
}

func (ii *InsertionIndex) InsertNoReplace(key cid.Cid, n uint64) {
	ii.items.InsertNoReplace(mkRecordFromCid(key, n))
}

type recordDigest struct {
	digest []byte
	Record
}

func (r recordDigest) Less(than llrb.Item) bool {
	other, ok := than.(recordDigest)
	if !ok {
		return false
	}
	return bytes.Compare(r.digest, other.digest) < 0
}

func mkRecord(r Record) recordDigest {
	d, err := multihash.Decode(r.Hash())
	if err != nil {
		return recordDigest{}
	}

	return recordDigest{d.Digest, r}
}

func mkRecordFromCid(c cid.Cid, at uint64) recordDigest {
	d, err := multihash.Decode(c.Hash())
	if err != nil {
		return recordDigest{}
	}

	return recordDigest{d.Digest, Record{Cid: c, Idx: at}}
}

func (ii *InsertionIndex) Get(c cid.Cid) (uint64, error) {
	d, err := multihash.Decode(c.Hash())
	if err != nil {
		return 0, err
	}
	entry := recordDigest{digest: d.Digest}
	e := ii.items.Get(entry)
	if e == nil {
		return 0, errNotFound
	}
	r, ok := e.(recordDigest)
	if !ok {
		return 0, errUnsupported
	}

	return r.Record.Idx, nil
}

func (ii *InsertionIndex) Marshal(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, int64(ii.items.Len())); err != nil {
		return err
	}

	var err error

	iter := func(i llrb.Item) bool {
		if err = cbor.Encode(w, i.(recordDigest).Record); err != nil {
			return false
		}
		return true
	}
	ii.items.AscendGreaterOrEqual(ii.items.Min(), iter)
	return err
}

func (ii *InsertionIndex) Unmarshal(r io.Reader) error {
	var len int64
	if err := binary.Read(r, binary.LittleEndian, &len); err != nil {
		return err
	}
	d := cbor.NewDecoder(r)
	for i := int64(0); i < len; i++ {
		var rec Record
		if err := d.Decode(&rec); err != nil {
			return err
		}
		ii.items.InsertNoReplace(mkRecord(rec))
	}
	return nil
}

// Codec identifies this index format
func (ii *InsertionIndex) Codec() Codec {
	return IndexInsertion
}

func (ii *InsertionIndex) Load(rs []Record) error {
	for _, r := range rs {
		rec := mkRecord(r)
		if rec.digest == nil {
			return fmt.Errorf("invalid entry: %v", r)
		}
		ii.items.InsertNoReplace(rec)
	}
	return nil
}

func mkInsertion() Index {
	ii := InsertionIndex{}
	return &ii
}

// Flatten returns a 'indexsorted' formatted index for more efficient subsequent loading
func (ii *InsertionIndex) Flatten() (Index, error) {
	si := BuildersByCodec[IndexSorted]()
	rcrds := make([]Record, ii.items.Len())

	idx := 0
	iter := func(i llrb.Item) bool {
		rcrds[idx] = i.(recordDigest).Record
		idx++
		return true
	}
	ii.items.AscendGreaterOrEqual(ii.items.Min(), iter)

	if err := si.Load(rcrds); err != nil {
		return nil, err
	}
	return si, nil
}
