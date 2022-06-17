package index

import (
	"encoding/binary"
	"errors"
	"io"
	"sort"

	"github.com/ipfs/go-cid"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

var (
	_ Index = (*MultihashIndexSorted)(nil)
)

type (
	// MultihashIndexSorted maps multihash code (i.e. hashing algorithm) to multiWidthCodedIndex.
	MultihashIndexSorted map[uint64]*multiWidthCodedIndex
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

func (m *multiWidthCodedIndex) Marshal(w io.Writer) (uint64, error) {
	if err := binary.Write(w, binary.LittleEndian, m.code); err != nil {
		return 8, err
	}
	n, err := m.multiWidthIndex.Marshal(w)
	return 8 + n, err
}

func (m *multiWidthCodedIndex) Unmarshal(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &m.code); err != nil {
		return err
	}
	return m.multiWidthIndex.Unmarshal(r)
}

func (m *multiWidthCodedIndex) UnmarshalLazyRead(r io.ReaderAt) (int64, error) {
	var b [8]byte
	_, err := internalio.FullReadAt(r, b[:], 0)
	if err != nil {
		return 0, err
	}
	m.code = binary.LittleEndian.Uint64(b[:8])
	rdr, err := internalio.NewOffsetReadSeekerWithError(r, int64(len(b)))
	if err != nil {
		return 0, err
	}
	sum, err := m.multiWidthIndex.UnmarshalLazyRead(rdr)
	if err != nil {
		return 0, err
	}
	oldSum := sum
	sum += int64(len(b))
	if sum < oldSum {
		return 0, errors.New("index too big; multiWidthCodedIndex len is overflowing")
	}
	return sum, nil
}

func (m *multiWidthCodedIndex) forEach(f func(mh multihash.Multihash, offset uint64) error) error {
	return m.multiWidthIndex.forEachDigest(func(digest []byte, offset uint64) error {
		mh, err := multihash.Encode(digest, m.code)
		if err != nil {
			return err
		}
		return f(mh, offset)
	})
}

func (m *MultihashIndexSorted) Codec() multicodec.Code {
	return multicodec.CarMultihashIndexSorted
}

func (m *MultihashIndexSorted) Marshal(w io.Writer) (uint64, error) {
	if err := binary.Write(w, binary.LittleEndian, int32(len(*m))); err != nil {
		return 4, err
	}
	// The codes are unique, but ranging over a map isn't deterministic.
	// As per the CARv2 spec, we must order buckets by digest length.
	// TODO update CARv2 spec to reflect this for the new index type.
	codes := m.sortedMultihashCodes()
	l := uint64(4)

	for _, code := range codes {
		mwci := (*m)[code]
		n, err := mwci.Marshal(w)
		l += n
		if err != nil {
			return l, err
		}
	}
	return l, nil
}

func (m *MultihashIndexSorted) sortedMultihashCodes() []uint64 {
	codes := make([]uint64, 0, len(*m))
	for code := range *m {
		codes = append(codes, code)
	}
	sort.Slice(codes, func(i, j int) bool {
		return codes[i] < codes[j]
	})
	return codes
}

func (m *MultihashIndexSorted) Unmarshal(r io.Reader) error {
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

func (m *MultihashIndexSorted) UnmarshalLazyRead(r io.ReaderAt) (sum int64, err error) {
	var b [4]byte
	_, err = internalio.FullReadAt(r, b[:], 0)
	if err != nil {
		return 0, err
	}
	sum += int64(len(b))
	count := binary.LittleEndian.Uint32(b[:4])
	if int32(count) < 0 {
		return 0, errors.New("index too big; MultihashIndexSorted count is overflowing int32")
	}
	for ; count > 0; count-- {
		mwci := newMultiWidthCodedIndex()
		or, err := internalio.NewOffsetReadSeekerWithError(r, sum)
		if err != nil {
			return 0, err
		}
		n, err := mwci.UnmarshalLazyRead(or)
		if err != nil {
			return 0, err
		}
		oldSum := sum
		sum += n
		if sum < oldSum {
			return 0, errors.New("index too big; MultihashIndexSorted sum is overflowing int64")
		}
		m.put(mwci)
	}
	return sum, nil
}

func (m *MultihashIndexSorted) put(mwci *multiWidthCodedIndex) {
	(*m)[mwci.code] = mwci
}

func (m *MultihashIndexSorted) Load(records []Record) error {
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

func (m *MultihashIndexSorted) GetAll(cid cid.Cid, f func(uint64) bool) error {
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

// ForEach calls f for every multihash and its associated offset stored by this index.
func (m *MultihashIndexSorted) ForEach(f func(mh multihash.Multihash, offset uint64) error) error {
	sizes := make([]uint64, 0, len(*m))
	for k := range *m {
		sizes = append(sizes, k)
	}
	sort.Slice(sizes, func(i, j int) bool { return sizes[i] < sizes[j] })
	for _, s := range sizes {
		mwci := (*m)[s]
		if err := mwci.forEach(f); err != nil {
			return err
		}
	}
	return nil
}

func (m *MultihashIndexSorted) get(dmh *multihash.DecodedMultihash) (*multiWidthCodedIndex, error) {
	if codedIdx, ok := (*m)[dmh.Code]; ok {
		return codedIdx, nil
	}
	return nil, ErrNotFound
}

func NewMultihashSorted() *MultihashIndexSorted {
	index := make(MultihashIndexSorted)
	return &index
}
