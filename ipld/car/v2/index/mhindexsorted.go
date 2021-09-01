package index

import (
	"encoding/binary"
	"io"
	"sort"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

type (
	// multihashIndexSorted maps multihash code (i.e. hashing algorithm) to multiWidthCodedIndex.
	// This index ignores any Record with multihash.IDENTITY.
	multihashIndexSorted map[uint64]*multiWidthCodedIndex
	// multiWidthCodedIndex stores multihash code for each multiWidthIndex.
	multiWidthCodedIndex struct {
		multiWidthIndex
		code uint64
	}
)

func newMultiWidthCodedIndex() *multiWidthCodedIndex {
	return &multiWidthCodedIndex{
		multiWidthIndex: make(multiWidthIndex),
	}
}

func (m *multiWidthCodedIndex) Marshal(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, m.code); err != nil {
		return err
	}
	return m.multiWidthIndex.Marshal(w)
}

func (m *multiWidthCodedIndex) Unmarshal(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &m.code); err != nil {
		return err
	}
	return m.multiWidthIndex.Unmarshal(r)
}

func (m *multihashIndexSorted) Codec() multicodec.Code {
	return multicodec.CarMultihashIndexSorted
}

func (m *multihashIndexSorted) Marshal(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, int32(len(*m))); err != nil {
		return err
	}
	// The codes are unique, but ranging over a map isn't deterministic.
	// As per the CARv2 spec, we must order buckets by digest length.
	// TODO update CARv2 spec to reflect this for the new index type.
	codes := m.sortedMultihashCodes()

	for _, code := range codes {
		mwci := (*m)[code]
		if err := mwci.Marshal(w); err != nil {
			return err
		}
	}
	return nil
}

func (m *multihashIndexSorted) sortedMultihashCodes() []uint64 {
	codes := make([]uint64, 0, len(*m))
	for code := range *m {
		codes = append(codes, code)
	}
	sort.Slice(codes, func(i, j int) bool {
		return codes[i] < codes[j]
	})
	return codes
}

func (m *multihashIndexSorted) Unmarshal(r io.Reader) error {
	var l int32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return err
	}
	for i := 0; i < int(l); i++ {
		mwci := newMultiWidthCodedIndex()
		if err := mwci.Unmarshal(r); err != nil {
			return err
		}
		m.put(mwci)
	}
	return nil
}

func (m *multihashIndexSorted) put(mwci *multiWidthCodedIndex) {
	(*m)[mwci.code] = mwci
}

func (m *multihashIndexSorted) Load(records []Record) error {
	// TODO optimize load by avoiding multihash decoding twice.
	// This implementation decodes multihashes twice: once here to group by code, and once in the
	// internals of multiWidthIndex to group by digest length. The code can be optimized by
	// combining the grouping logic into one step where the multihash of a CID is only decoded once.
	// The optimization would need refactoring of the IndexSorted compaction logic.

	// Group records by multihash code
	byCode := make(map[uint64][]Record)
	for _, record := range records {
		dmh, err := multihash.Decode(record.Hash())
		if err != nil {
			return err
		}
		code := dmh.Code
		// Ignore IDENTITY multihash in the index.
		if code == multihash.IDENTITY {
			continue
		}
		recsByCode, ok := byCode[code]
		if !ok {
			recsByCode = make([]Record, 0)
			byCode[code] = recsByCode
		}
		byCode[code] = append(recsByCode, record)
	}

	// Load each record group.
	for code, recsByCode := range byCode {
		mwci := newMultiWidthCodedIndex()
		mwci.code = code
		if err := mwci.Load(recsByCode); err != nil {
			return err
		}
		m.put(mwci)
	}
	return nil
}

func (m *multihashIndexSorted) GetAll(cid cid.Cid, f func(uint64) bool) error {
	hash := cid.Hash()
	dmh, err := multihash.Decode(hash)
	if err != nil {
		return err
	}
	mwci, err := m.get(dmh)
	if err != nil {
		return err
	}
	return mwci.GetAll(cid, f)
}

func (m *multihashIndexSorted) get(dmh *multihash.DecodedMultihash) (*multiWidthCodedIndex, error) {
	if codedIdx, ok := (*m)[dmh.Code]; ok {
		return codedIdx, nil
	}
	return nil, ErrNotFound
}

func newMultihashSorted() Index {
	index := make(multihashIndexSorted)
	return &index
}
