package blockstore

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	carv1 "github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"golang.org/x/exp/mmap"
)

var _ blockstore.Blockstore = (*ReadOnly)(nil)

// errUnsupported is returned for unsupported operations
var errUnsupported = errors.New("unsupported operation")

// ReadOnly provides a read-only Car Block Store.
type ReadOnly struct {
	backing io.ReaderAt
	idx     index.Index
}

// ReadOnlyOf opens a carbs data store from an existing backing of the base data (i.e. CAR v1 payload) and index.
func ReadOnlyOf(backing io.ReaderAt, index index.Index) *ReadOnly {
	return &ReadOnly{backing, index}
}

// OpenReadOnly opens a read-only blockstore from a CAR v2 file, generating an index if it does not exist.
// If noPersist is set to false then the generated index is written into the CAR v2 file at path.
func OpenReadOnly(path string, noPersist bool) (*ReadOnly, error) {
	reader, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}

	v2r, err := carv2.NewReader(reader)
	if err != nil {
		return nil, err
	}
	var idx index.Index
	if !v2r.Header.HasIndex() {
		idx, err := index.Generate(v2r.CarV1Reader(), index.IndexSorted)
		if err != nil {
			return nil, err
		}
		if !noPersist {
			if err = index.Save(idx, path); err != nil {
				return nil, err
			}
		}
	} else {
		idx, err = index.ReadFrom(v2r.IndexReader())
		if err != nil {
			return nil, err
		}
	}
	obj := ReadOnly{
		backing: v2r.CarV1Reader(),
		idx:     idx,
	}
	return &obj, nil
}

func (b *ReadOnly) read(idx int64) (cid.Cid, []byte, error) {
	bcid, data, err := util.ReadNode(bufio.NewReader(internalio.NewOffsetReader(b.backing, idx)))
	return bcid, data, err
}

// DeleteBlock is unsupported and always returns an error
func (b *ReadOnly) DeleteBlock(_ cid.Cid) error {
	return errUnsupported
}

// Has indicates if the store has a cid
func (b *ReadOnly) Has(key cid.Cid) (bool, error) {
	offset, err := b.idx.Get(key)
	if err != nil {
		return false, err
	}
	uar := internalio.NewOffsetReader(b.backing, int64(offset))
	_, err = binary.ReadUvarint(uar)
	if err != nil {
		return false, err
	}
	c, _, err := internalio.ReadCid(b.backing, uar.Offset())
	if err != nil {
		return false, err
	}
	return c.Equals(key), nil
}

// Get gets a block from the store
func (b *ReadOnly) Get(key cid.Cid) (blocks.Block, error) {
	offset, err := b.idx.Get(key)
	if err != nil {
		return nil, err
	}
	entry, bytes, err := b.read(int64(offset))
	if err != nil {
		// TODO Improve error handling; not all errors mean NotFound.
		return nil, blockstore.ErrNotFound
	}
	if !entry.Equals(key) {
		return nil, blockstore.ErrNotFound
	}
	return blocks.NewBlockWithCid(bytes, key)
}

// GetSize gets how big a item is
func (b *ReadOnly) GetSize(key cid.Cid) (int, error) {
	idx, err := b.idx.Get(key)
	if err != nil {
		return -1, err
	}
	l, err := binary.ReadUvarint(internalio.NewOffsetReader(b.backing, int64(idx)))
	if err != nil {
		return -1, blockstore.ErrNotFound
	}
	c, _, err := internalio.ReadCid(b.backing, int64(idx+l))
	if err != nil {
		return 0, err
	}
	if !c.Equals(key) {
		return -1, blockstore.ErrNotFound
	}
	// get cid. validate.
	return int(l), err
}

// Put is not supported and always returns an error
func (b *ReadOnly) Put(blocks.Block) error {
	return errUnsupported
}

// PutMany is not supported and always returns an error
func (b *ReadOnly) PutMany([]blocks.Block) error {
	return errUnsupported
}

// AllKeysChan returns the list of keys in the store
func (b *ReadOnly) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	// TODO we may use this walk for populating the index, and we need to be able to iterate keys in this way somewhere for index generation. In general though, when it's asked for all keys from a blockstore with an index, we should iterate through the index when possible rather than linear reads through the full car.
	header, err := carv1.ReadHeader(bufio.NewReader(internalio.NewOffsetReader(b.backing, 0)))
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

		rdr := internalio.NewOffsetReader(b.backing, int64(offset))
		for {
			l, err := binary.ReadUvarint(rdr)
			thisItemForNxt := rdr.Offset()
			if err != nil {
				return
			}
			c, _, err := internalio.ReadCid(b.backing, thisItemForNxt)
			if err != nil {
				return
			}
			rdr.SeekOffset(thisItemForNxt + int64(l))

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
func (b *ReadOnly) HashOnRead(bool) {
}

// Roots returns the root CIDs of the backing car
func (b *ReadOnly) Roots() ([]cid.Cid, error) {
	header, err := carv1.ReadHeader(bufio.NewReader(internalio.NewOffsetReader(b.backing, 0)))
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}
	return header.Roots, nil
}
