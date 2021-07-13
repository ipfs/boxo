package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/multiformats/go-multicodec"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

type (
	digestRecord struct {
		digest []byte
		index  uint64
	}
	recordSet        []digestRecord
	singleWidthIndex struct {
		width uint32
		len   uint64 // in struct, len is #items. when marshaled, it's saved as #bytes.
		index []byte
	}
	multiWidthIndex map[uint32]singleWidthIndex
)

func (d digestRecord) write(buf []byte) {
	n := copy(buf[:], d.digest)
	binary.LittleEndian.PutUint64(buf[n:], d.index)
}

func (r recordSet) Len() int {
	return len(r)
}

func (r recordSet) Less(i, j int) bool {
	return bytes.Compare(r[i].digest, r[j].digest) < 0
}

func (r recordSet) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (s *singleWidthIndex) Codec() multicodec.Code {
	return multicodec.Code(indexSingleSorted)
}

func (s *singleWidthIndex) Marshal(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, s.width); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, int64(len(s.index))); err != nil {
		return err
	}
	// TODO: we could just w.Write(s.index) here and avoid overhead
	_, err := io.Copy(w, bytes.NewBuffer(s.index))
	return err
}

func (s *singleWidthIndex) Unmarshal(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &s.width); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &s.len); err != nil {
		return err
	}
	s.index = make([]byte, s.len)
	s.len /= uint64(s.width)
	_, err := io.ReadFull(r, s.index)
	return err
}

func (s *singleWidthIndex) Less(i int, digest []byte) bool {
	return bytes.Compare(digest[:], s.index[i*int(s.width):((i+1)*int(s.width)-8)]) <= 0
}

func (s *singleWidthIndex) Get(c cid.Cid) (uint64, error) {
	d, err := multihash.Decode(c.Hash())
	if err != nil {
		return 0, err
	}
	return s.get(d.Digest), nil
}

func (s *singleWidthIndex) get(d []byte) uint64 {
	idx := sort.Search(int(s.len), func(i int) bool {
		return s.Less(i, d)
	})
	if uint64(idx) == s.len {
		return 0
	}
	if !bytes.Equal(d[:], s.index[idx*int(s.width):(idx+1)*int(s.width)-8]) {
		return 0
	}
	return binary.LittleEndian.Uint64(s.index[(idx+1)*int(s.width)-8 : (idx+1)*int(s.width)])
}

func (s *singleWidthIndex) Load(items []Record) error {
	m := make(multiWidthIndex)
	if err := m.Load(items); err != nil {
		return err
	}
	if len(m) != 1 {
		return fmt.Errorf("unexpected number of cid widths: %d", len(m))
	}
	for _, i := range m {
		s.index = i.index
		s.len = i.len
		s.width = i.width
		return nil
	}
	return nil
}

func (m *multiWidthIndex) Get(c cid.Cid) (uint64, error) {
	d, err := multihash.Decode(c.Hash())
	if err != nil {
		return 0, err
	}
	if s, ok := (*m)[uint32(len(d.Digest)+8)]; ok {
		return s.get(d.Digest), nil
	}
	return 0, ErrNotFound
}

func (m *multiWidthIndex) Codec() multicodec.Code {
	return multicodec.CarIndexSorted
}

func (m *multiWidthIndex) Marshal(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, int32(len(*m))); err != nil {
		return err
	}

	// The widths are unique, but ranging over a map isn't deterministic.
	// As per the CARv2 spec, we must order buckets by digest length.

	widths := make([]uint32, 0, len(*m))
	for width := range *m {
		widths = append(widths, width)
	}
	sort.Slice(widths, func(i, j int) bool {
		return widths[i] < widths[j]
	})

	for _, width := range widths {
		bucket := (*m)[width]
		if err := bucket.Marshal(w); err != nil {
			return err
		}
	}
	return nil
}

func (m *multiWidthIndex) Unmarshal(r io.Reader) error {
	var l int32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return err
	}
	for i := 0; i < int(l); i++ {
		s := singleWidthIndex{}
		if err := s.Unmarshal(r); err != nil {
			return err
		}
		(*m)[s.width] = s
	}
	return nil
}

func (m *multiWidthIndex) Load(items []Record) error {
	// Split cids on their digest length
	idxs := make(map[int][]digestRecord)
	for _, item := range items {
		decHash, err := multihash.Decode(item.Hash())
		if err != nil {
			return err
		}
		digest := decHash.Digest
		idx, ok := idxs[len(digest)]
		if !ok {
			idxs[len(digest)] = make([]digestRecord, 0)
			idx = idxs[len(digest)]
		}
		idxs[len(digest)] = append(idx, digestRecord{digest, item.Idx})
	}

	// Sort each list. then write to compact form.
	for width, lst := range idxs {
		sort.Sort(recordSet(lst))
		rcrdWdth := width + 8
		compact := make([]byte, rcrdWdth*len(lst))
		for off, itm := range lst {
			itm.write(compact[off*rcrdWdth : (off+1)*rcrdWdth])
		}
		s := singleWidthIndex{
			width: uint32(rcrdWdth),
			len:   uint64(len(lst)),
			index: compact,
		}
		(*m)[uint32(width)+8] = s
	}
	return nil
}

func newSorted() Index {
	m := make(multiWidthIndex)
	return &m
}

//lint:ignore U1000 kept for potential future use.
func newSingleSorted() Index {
	s := singleWidthIndex{}
	return &s
}
