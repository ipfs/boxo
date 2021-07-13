package index

import (
	"io"

	"github.com/multiformats/go-multicodec"

	"github.com/ipfs/go-cid"
	cbor "github.com/whyrusleeping/cbor/go"
)

//lint:ignore U1000 kept for potential future use.
type mapIndex map[cid.Cid]uint64

func (m *mapIndex) Get(c cid.Cid) (uint64, error) {
	el, ok := (*m)[c]
	if !ok {
		return 0, ErrNotFound
	}
	return el, nil
}

func (m *mapIndex) Marshal(w io.Writer) error {
	return cbor.Encode(w, m)
}

func (m *mapIndex) Unmarshal(r io.Reader) error {
	d := cbor.NewDecoder(r)
	return d.Decode(m)
}

func (m *mapIndex) Codec() multicodec.Code {
	return multicodec.Code(indexHashed)
}

func (m *mapIndex) Load(rs []Record) error {
	for _, r := range rs {
		(*m)[r.Cid] = r.Idx
	}
	return nil
}

//lint:ignore U1000 kept for potential future use.
func newHashed() Index {
	mi := make(mapIndex)
	return &mi
}
