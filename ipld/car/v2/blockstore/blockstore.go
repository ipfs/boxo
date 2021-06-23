package blockstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	carv1 "github.com/ipld/go-car"
	carv2 "github.com/ipld/go-car/v2"
	internalio "github.com/ipld/go-car/v2/internal/io"

	"github.com/ipld/go-car/v2/index"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/util"
)

var _ (blockstore.Blockstore) = (*Blockstore)(nil)
var errFinalized = errors.New("finalized blockstore")

// Blockstore is a carbon implementation based on having two file handles opened,
// one appending to the file, and the other
// seeking to read items as needed.
// This implementation is preferable for a write-heavy workload.
// The Finalize function must be called once the putting blocks are finished.
// Upon calling Finalize all read and write calls to this blockstore will result in error.
type (
	Blockstore struct {
		w           io.WriterAt
		carV1Wrtier *internalio.OffsetWriter
		ReadOnly
		idx    *index.InsertionIndex
		header carv2.Header
	}
	Option func(*Blockstore)
)

func WithCarV1Padding(p uint64) Option {
	return func(b *Blockstore) {
		b.header = b.header.WithCarV1Padding(p)
	}
}

func WithIndexPadding(p uint64) Option {
	return func(b *Blockstore) {
		b.header = b.header.WithIndexPadding(p)
	}
}

// New creates a new Blockstore at the given path with a provided set of root cids as the car roots.
func New(path string, roots []cid.Cid, opts ...Option) (*Blockstore, error) {
	// TODO support resumption if the path provided contains partially written blocks in v2 format.
	wfd, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o666)
	if err != nil {
		return nil, fmt.Errorf("couldn't create backing car: %w", err)
	}
	rfd, err := os.OpenFile(path, os.O_RDONLY, 0o666)
	if err != nil {
		return nil, fmt.Errorf("could not re-open read handle: %w", err)
	}

	indexcls, ok := index.IndexAtlas[index.IndexInsertion]
	if !ok {
		return nil, fmt.Errorf("unknownindex  codec: %#v", index.IndexInsertion)
	}
	idx := (indexcls()).(*index.InsertionIndex)

	b := &Blockstore{
		w:        wfd,
		ReadOnly: *ReadOnlyOf(rfd, idx),
		idx:      idx,
		header:   carv2.Header{},
	}

	applyOptions(b, opts)
	b.carV1Wrtier = internalio.NewOffsetWriter(wfd, int64(b.header.CarV1Offset))

	if _, err := wfd.Write(carv2.Pragma); err != nil {
		return nil, err
	}

	v1Header := &carv1.CarHeader{
		Roots:   roots,
		Version: 1,
	}
	if err := carv1.WriteHeader(v1Header, b.carV1Wrtier); err != nil {
		return nil, fmt.Errorf("couldn't write car header: %w", err)
	}
	return b, nil
}

func applyOptions(b *Blockstore, opts []Option) {
	for _, opt := range opts {
		opt(b)
	}
}

func (b *Blockstore) DeleteBlock(cid.Cid) error {
	return errUnsupported
}

// Put puts a given block to the underlying datastore
func (b *Blockstore) Put(blk blocks.Block) error {
	if b.isFinalized() {
		return errFinalized
	}
	return b.PutMany([]blocks.Block{blk})
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (b *Blockstore) PutMany(blks []blocks.Block) error {
	if b.isFinalized() {
		return errFinalized
	}
	for _, bl := range blks {
		n := uint64(b.carV1Wrtier.Position())
		if err := util.LdWrite(b.carV1Wrtier, bl.Cid().Bytes(), bl.RawData()); err != nil {
			return err
		}
		b.idx.InsertNoReplace(bl.Cid(), n)
	}
	return nil
}

func (b *Blockstore) isFinalized() bool {
	return b.header.CarV1Size != 0
}

// Finalize finalizes this blockstore by writing the CAR v2 header, along with flattened index
// for more efficient subsequent read.
// After this call, this blockstore can no longer be used for read or write.
func (b *Blockstore) Finalize() error {
	if b.isFinalized() {
		return errFinalized
	}
	// TODO check if add index option is set and don't write the index then set index offset to zero.
	// TODO see if folks need to continue reading from a finalized blockstore, if so return ReadOnly blockstore here.
	b.header.CarV1Size = uint64(b.carV1Wrtier.Position())
	if _, err := b.header.WriteTo(internalio.NewOffsetWriter(b.w, carv2.PragmaSize)); err != nil {
		return err
	}
	// TODO if index not needed don't bother flattening it.
	fi, err := b.idx.Flatten()
	if err != nil {
		return err
	}
	return index.WriteTo(fi, internalio.NewOffsetWriter(b.w, int64(b.header.IndexOffset)))
}

func (b *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	if b.isFinalized() {
		return nil, errFinalized
	}
	return b.ReadOnly.AllKeysChan(ctx)
}

func (b *Blockstore) Has(key cid.Cid) (bool, error) {
	if b.isFinalized() {
		return false, errFinalized
	}
	return b.ReadOnly.Has(key)
}

func (b *Blockstore) Get(key cid.Cid) (blocks.Block, error) {
	if b.isFinalized() {
		return nil, errFinalized
	}
	return b.ReadOnly.Get(key)
}

func (b *Blockstore) GetSize(key cid.Cid) (int, error) {
	if b.isFinalized() {
		return 0, errFinalized
	}
	return b.ReadOnly.GetSize(key)
}
