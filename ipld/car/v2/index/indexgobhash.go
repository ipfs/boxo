package index

import (
	"encoding/gob"
	"io"

	"github.com/multiformats/go-multicodec"

	"github.com/ipfs/go-cid"
)

//lint:ignore U1000 kept for potential future use.
type mapGobIndex map[cid.Cid]uint64

func (m *mapGobIndex) Get(c cid.Cid) (uint64, error) {
	el, ok := (*m)[c]
	if !ok {
		return 0, ErrNotFound
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

func (m *mapGobIndex) Codec() multicodec.Code {
	return multicodec.Code(indexHashed)
}

func (m *mapGobIndex) Load(rs []Record) error {
	for _, r := range rs {
		(*m)[r.Cid] = r.Idx
	}
	return nil
}

//lint:ignore U1000 kept for potential future use.
func newGobHashed() Index {
	mi := make(mapGobIndex)
	return &mi
}
