package carbs

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/multiformats/go-multihash"

	pb "github.com/cheggaaa/pb/v3"
	carv1 "github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	"golang.org/x/exp/mmap"
)

var errNotFound = blockstore.ErrNotFound

// BlockStore provides a read-only Car Block Store.
type BlockStore struct {
	backing io.ReaderAt
	idx     Index
}

var _ blockstore.Blockstore = (*BlockStore)(nil)

func (b *BlockStore) Read(idx int64) (cid.Cid, []byte, error) {
	bcid, data, err := util.ReadNode(bufio.NewReader(&unatreader{b.backing, idx}))
	return bcid, data, err
}

// DeleteBlock doesn't delete a block on RO blockstore
func (b *BlockStore) DeleteBlock(_ cid.Cid) error {
	return fmt.Errorf("read only")
}

// Has indicates if the store has a cid
func (b *BlockStore) Has(key cid.Cid) (bool, error) {
	offset, err := b.idx.Get(key)
	if err != nil {
		return false, err
	}
	uar := unatreader{b.backing, int64(offset)}
	_, err = binary.ReadUvarint(&uar)
	if err != nil {
		return false, err
	}
	cid, _, err := readCid(b.backing, uar.at)
	if err != nil {
		return false, err
	}
	return cid.Equals(key), nil
}

var cidv0Pref = []byte{0x12, 0x20}

func readCid(store io.ReaderAt, at int64) (cid.Cid, int, error) {
	var tag [2]byte
	if _, err := store.ReadAt(tag[:], at); err != nil {
		return cid.Undef, 0, err
	}
	if bytes.Equal(tag[:], cidv0Pref) {
		cid0 := make([]byte, 34)
		if _, err := store.ReadAt(cid0, at); err != nil {
			return cid.Undef, 0, err
		}
		c, err := cid.Cast(cid0)
		return c, 34, err
	}

	// assume cidv1
	br := &unatreader{store, at}
	vers, err := binary.ReadUvarint(br)
	if err != nil {
		return cid.Cid{}, 0, err
	}

	// TODO: the go-cid package allows version 0 here as well
	if vers != 1 {
		return cid.Cid{}, 0, fmt.Errorf("invalid cid version number: %d", vers)
	}

	codec, err := binary.ReadUvarint(br)
	if err != nil {
		return cid.Cid{}, 0, err
	}

	mhr := multihash.NewReader(br)
	h, err := mhr.ReadMultihash()
	if err != nil {
		return cid.Cid{}, 0, err
	}

	return cid.NewCidV1(codec, h), int(br.at - at), nil
}

// Get gets a block from the store
func (b *BlockStore) Get(key cid.Cid) (blocks.Block, error) {
	offset, err := b.idx.Get(key)
	if err != nil {
		return nil, err
	}
	entry, bytes, err := b.Read(int64(offset))
	if err != nil {
		// TODO replace with logging
		fmt.Printf("failed get %d:%v\n", offset, err)
		return nil, blockstore.ErrNotFound
	}
	if !entry.Equals(key) {
		return nil, blockstore.ErrNotFound
	}
	return blocks.NewBlockWithCid(bytes, key)
}

// GetSize gets how big a item is
func (b *BlockStore) GetSize(key cid.Cid) (int, error) {
	idx, err := b.idx.Get(key)
	if err != nil {
		return -1, err
	}
	len, err := binary.ReadUvarint(&unatreader{b.backing, int64(idx)})
	if err != nil {
		return -1, blockstore.ErrNotFound
	}
	cid, _, err := readCid(b.backing, int64(idx+len))
	if err != nil {
		return 0, err
	}
	if !cid.Equals(key) {
		return -1, blockstore.ErrNotFound
	}
	// get cid. validate.
	return int(len), err
}

// Put does nothing on a ro store
func (b *BlockStore) Put(blocks.Block) error {
	return fmt.Errorf("read only")
}

// PutMany does nothing on a ro store
func (b *BlockStore) PutMany([]blocks.Block) error {
	return fmt.Errorf("read only")
}

// AllKeysChan returns the list of keys in the store
func (b *BlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	header, err := carv1.ReadHeader(bufio.NewReader(&unatreader{b.backing, 0}))
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}
	offset, err := carv1.HeaderSize(header)
	if err != nil {
		return nil, err
	}

	ch := make(chan cid.Cid, 5)
	go func() {
		done := ctx.Done()

		rdr := unatreader{b.backing, int64(offset)}
		for {
			l, err := binary.ReadUvarint(&rdr)
			thisItemForNxt := rdr.at
			if err != nil {
				return
			}
			c, _, err := readCid(b.backing, thisItemForNxt)
			if err != nil {
				return
			}
			rdr.at = thisItemForNxt + int64(l)

			select {
			case ch <- c:
				continue
			case <-done:
				return
			}
		}
	}()
	return ch, nil
}

// HashOnRead does nothing
func (b *BlockStore) HashOnRead(bool) {
}

// Roots returns the root CIDs of the backing car
func (b *BlockStore) Roots() ([]cid.Cid, error) {
	header, err := carv1.ReadHeader(bufio.NewReader(&unatreader{b.backing, 0}))
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}
	return header.Roots, nil
}

// Load opens a carbs data store, generating an index if it does not exist
func Load(path string, noPersist bool) (*BlockStore, error) {
	reader, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}
	idx, err := Restore(path)
	if err != nil {
		idx, err = GenerateIndex(reader, 0, IndexSorted, false)
		if err != nil {
			return nil, err
		}
		if !noPersist {
			if err = Save(idx, path); err != nil {
				return nil, err
			}
		}
	}
	obj := BlockStore{
		backing: reader,
		idx:     idx,
	}
	return &obj, nil
}

// Of opens a carbs data store from an existing reader of the base data and index
func Of(backing io.ReaderAt, index Index) *BlockStore {
	return &BlockStore{backing, index}
}

// GenerateIndex provides a low-level interface to create an index over a
// reader to a car stream.
func GenerateIndex(store io.ReaderAt, size int64, codec IndexCodec, verbose bool) (Index, error) {
	indexcls, ok := IndexAtlas[codec]
	if !ok {
		return nil, fmt.Errorf("unknown codec: %#v", codec)
	}

	bar := pb.New64(size)
	bar.Set(pb.Bytes, true)
	bar.Set(pb.Terminal, true)

	bar.Start()

	header, err := carv1.ReadHeader(bufio.NewReader(&unatreader{store, 0}))
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}
	offset, err := carv1.HeaderSize(header)
	if err != nil {
		return nil, err
	}
	bar.Add64(int64(offset))

	index := indexcls()

	records := make([]Record, 0)
	rdr := unatreader{store, int64(offset)}
	for {
		thisItemIdx := rdr.at
		l, err := binary.ReadUvarint(&rdr)
		bar.Add64(int64(l))
		thisItemForNxt := rdr.at
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		c, _, err := readCid(store, thisItemForNxt)
		if err != nil {
			return nil, err
		}
		records = append(records, Record{c, uint64(thisItemIdx)})
		rdr.at = thisItemForNxt + int64(l)
	}

	if err := index.Load(records); err != nil {
		return nil, err
	}

	bar.Finish()

	return index, nil
}

// Generate walks a car file and generates an index of cid->byte offset in it.
func Generate(path string, codec IndexCodec) error {
	store, err := mmap.Open(path)
	if err != nil {
		return err
	}
	idx, err := GenerateIndex(store, 0, codec, false)
	if err != nil {
		return err
	}

	return Save(idx, path)
}
