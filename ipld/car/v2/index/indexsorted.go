package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/ipld/go-car/v2/internal/errsort"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/multiformats/go-multicodec"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

type sizedReaderAt interface {
	io.ReaderAt
	Size() int64
}

var _ Index = (*multiWidthIndex)(nil)

type (
	digestRecord struct {
		digest []byte
		index  uint64
	}
	recordSet        []digestRecord
	singleWidthIndex struct {
		width uint32
		len   uint64 // in struct, len is #items. when marshaled, it's saved as #bytes.
		index sizedReaderAt
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

func (s *singleWidthIndex) Marshal(w io.Writer) (uint64, error) {
	l := uint64(0)
	if err := binary.Write(w, binary.LittleEndian, s.width); err != nil {
		return 0, err
	}
	l += 4
	sz := s.index.Size()
	if err := binary.Write(w, binary.LittleEndian, sz); err != nil {
		return l, err
	}
	l += 8
	n, err := io.Copy(w, io.NewSectionReader(s.index, 0, sz))
	return l + uint64(n), err
}

// Unmarshal decodes the index from its serial form.
// Deprecated: This function is slurpy and will copy the index in memory.
func (s *singleWidthIndex) Unmarshal(r io.Reader) error {
	var width uint32
	if err := binary.Read(r, binary.LittleEndian, &width); err != nil {
		return err
	}
	var dataLen uint64
	if err := binary.Read(r, binary.LittleEndian, &dataLen); err != nil {
		return err
	}

	if err := s.checkUnmarshalLengths(width, dataLen, 0); err != nil {
		return err
	}

	buf := make([]byte, dataLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	s.index = bytes.NewReader(buf)
	return nil
}

func (s *singleWidthIndex) UnmarshalLazyRead(r io.ReaderAt) (indexSize int64, err error) {
	var b [12]byte
	_, err = internalio.FullReadAt(r, b[:], 0)
	if err != nil {
		return 0, err
	}

	width := binary.LittleEndian.Uint32(b[:4])
	dataLen := binary.LittleEndian.Uint64(b[4:12])
	if err := s.checkUnmarshalLengths(width, dataLen, uint64(len(b))); err != nil {
		return 0, err
	}
	s.index = io.NewSectionReader(r, int64(len(b)), int64(dataLen))
	return int64(dataLen) + int64(len(b)), nil
}

func (s *singleWidthIndex) checkUnmarshalLengths(width uint32, dataLen, extra uint64) error {
	if width <= 8 {
		return errors.New("malformed index; width must be bigger than 8")
	}
	if int32(width) < 0 {
		return errors.New("index too big; singleWidthIndex width is overflowing int32")
	}
	oldDataLen, dataLen := dataLen, dataLen+extra
	if oldDataLen > dataLen {
		return errors.New("index too big; singleWidthIndex len is overflowing")
	}
	if int64(dataLen) < 0 {
		return errors.New("index too big; singleWidthIndex len is overflowing int64")
	}
	s.width = width
	s.len = dataLen / uint64(width)
	return nil
}

func (s *singleWidthIndex) GetAll(c cid.Cid, fn func(uint64) bool) error {
	d, err := multihash.Decode(c.Hash())
	if err != nil {
		return err
	}
	return s.getAll(d.Digest, fn)
}

func (s *singleWidthIndex) getAll(d []byte, fn func(uint64) bool) error {
	digestLen := int64(s.width) - 8
	b := make([]byte, digestLen)
	idxI, err := errsort.Search(int(s.len), func(i int) (bool, error) {
		digestStart := int64(i) * int64(s.width)
		_, err := internalio.FullReadAt(s.index, b, digestStart)
		if err != nil {
			return false, err
		}
		return bytes.Compare(d, b) <= 0, nil
	})
	if err != nil {
		return err
	}
	idx := int64(idxI)

	var any bool
	for ; uint64(idx) < s.len; idx++ {
		digestStart := idx * int64(s.width)
		offsetEnd := digestStart + int64(s.width)
		digestEnd := offsetEnd - 8
		digestLen := digestEnd - digestStart
		b := make([]byte, offsetEnd-digestStart)
		_, err := internalio.FullReadAt(s.index, b, digestStart)
		if err != nil {
			return err
		}
		if bytes.Equal(d, b[:digestLen]) {
			any = true
			offset := binary.LittleEndian.Uint64(b[digestLen:])
			if !fn(offset) {
				// User signalled to stop searching; therefore, break.
				break
			}
		} else {
			// No more matches; therefore, break.
			break
		}
	}
	if !any {
		return ErrNotFound
	}
	return nil
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

func (s *singleWidthIndex) forEachDigest(f func(digest []byte, offset uint64) error) error {
	segmentCount := s.index.Size() / int64(s.width)
	for i := int64(0); i < segmentCount; i++ {
		digestStart := i * int64(s.width)
		offsetEnd := digestStart + int64(s.width)
		digestEnd := offsetEnd - 8
		digestLen := digestEnd - digestStart
		b := make([]byte, offsetEnd-digestStart)
		_, err := internalio.FullReadAt(s.index, b, digestStart)
		if err != nil {
			return err
		}
		digest := b[:digestLen]
		offset := binary.LittleEndian.Uint64(b[digestLen:])
		if err := f(digest, offset); err != nil {
			return err
		}
	}
	return nil
}

func (m *multiWidthIndex) GetAll(c cid.Cid, fn func(uint64) bool) error {
	d, err := multihash.Decode(c.Hash())
	if err != nil {
		return err
	}
	if s, ok := (*m)[uint32(len(d.Digest)+8)]; ok {
		return s.getAll(d.Digest, fn)
	}
	return ErrNotFound
}

func (m *multiWidthIndex) Codec() multicodec.Code {
	return multicodec.CarIndexSorted
}

func (m *multiWidthIndex) Marshal(w io.Writer) (uint64, error) {
	l := uint64(0)
	if err := binary.Write(w, binary.LittleEndian, int32(len(*m))); err != nil {
		return l, err
	}
	l += 4

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
		n, err := bucket.Marshal(w)
		l += n
		if err != nil {
			return l, err
		}
	}
	return l, nil
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

func (m *multiWidthIndex) UnmarshalLazyRead(r io.ReaderAt) (sum int64, err error) {
	var b [4]byte
	_, err = internalio.FullReadAt(r, b[:], 0)
	if err != nil {
		return 0, err
	}
	count := binary.LittleEndian.Uint32(b[:4])
	if int32(count) < 0 {
		return 0, errors.New("index too big; multiWidthIndex count is overflowing int32")
	}
	sum += int64(len(b))
	for ; count > 0; count-- {
		s := singleWidthIndex{}
		or, err := internalio.NewOffsetReadSeekerWithError(r, sum)
		if err != nil {
			return 0, err
		}
		n, err := s.UnmarshalLazyRead(or)
		if err != nil {
			return 0, err
		}
		oldSum := sum
		sum += n
		if sum < oldSum {
			return 0, errors.New("index too big; multiWidthIndex len is overflowing int64")
		}
		(*m)[s.width] = s
	}
	return sum, nil
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
		idxs[len(digest)] = append(idx, digestRecord{digest, item.Offset})
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
			index: bytes.NewReader(compact),
		}
		(*m)[uint32(width)+8] = s
	}
	return nil
}

func (m *multiWidthIndex) ForEach(f func(multihash.Multihash, uint64) error) error {
	return m.forEachDigest(func(digest []byte, offset uint64) error {
		mh, err := multihash.Cast(digest)
		if err != nil {
			return err
		}
		return f(mh, offset)
	})
}

func (m *multiWidthIndex) forEachDigest(f func(digest []byte, offset uint64) error) error {
	sizes := make([]uint32, 0, len(*m))
	for k := range *m {
		sizes = append(sizes, k)
	}
	sort.Slice(sizes, func(i, j int) bool { return sizes[i] < sizes[j] })
	for _, s := range sizes {
		swi := (*m)[s]
		if err := swi.forEachDigest(f); err != nil {
			return err
		}
	}
	return nil
}

func newSorted() Index {
	m := make(multiWidthIndex)
	return &m
}
