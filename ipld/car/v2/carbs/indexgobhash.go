package carbs

import (
	"encoding/gob"
	"io"

	"github.com/ipfs/go-cid"
)

type mapGobIndex map[cid.Cid]uint64

func (m *mapGobIndex) Get(c cid.Cid) (uint64, error) {
	el, ok := (*m)[c]
	if !ok {
		return 0, errNotFound
	}
	return el, nil
}

func (m *mapGobIndex) Marshal(w io.Writer) error {
	e := gob.NewEncoder(w)
	return e.Encode(m)
}

func (m *mapGobIndex) Unmarshal(r io.Reader) error {
	d := gob.NewDecoder(r)
	return d.Decode(m)
}

func (m *mapGobIndex) Codec() IndexCodec {
	return IndexHashed
}

func (m *mapGobIndex) Load(rs []Record) error {
	for _, r := range rs {
		(*m)[r.Cid] = r.Idx
	}
	return nil
}

func mkGobHashed() Index {
	mi := make(mapGobIndex)
	return &mi
}
