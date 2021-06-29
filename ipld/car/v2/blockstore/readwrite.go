package blockstore

import (
	"context"
	"fmt"
	"os"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/internal/carv1"
	internalio "github.com/ipld/go-car/v2/internal/io"

	"github.com/ipld/go-car/v2/index"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/internal/carv1/util"
)

var _ blockstore.Blockstore = (*ReadWrite)(nil)

// ReadWrite implements a blockstore that stores blocks in CAR v2 format.
// Blocks put into the blockstore can be read back once they are successfully written.
// This implementation is preferable for a write-heavy workload.
// The blocks are written immediately on Put and PutAll calls, while the index is stored in memory
// and updated incrementally.
// The Finalize function must be called once the putting blocks are finished.
// Upon calling Finalize header is finalized and index is written out.
// Once finalized, all read and write calls to this blockstore will result in panics.
type ReadWrite struct {
	f           *os.File
	carV1Writer *internalio.OffsetWriter
	ReadOnly
	idx    *index.InsertionIndex
	header carv2.Header
}

// TODO consider exposing interfaces
type Option func(*ReadWrite) // TODO consider unifying with writer options

// WithCarV1Padding sets the padding to be added between CAR v2 header and its data payload on Finalize.
func WithCarV1Padding(p uint64) Option {
	return func(b *ReadWrite) {
		b.header = b.header.WithCarV1Padding(p)
	}
}

// WithIndexPadding sets the padding between data payload and its index on Finalize.
func WithIndexPadding(p uint64) Option {
	return func(b *ReadWrite) {
		b.header = b.header.WithIndexPadding(p)
	}
}

// NewReadWrite creates a new ReadWrite at the given path with a provided set of root CIDs as the car roots.
func NewReadWrite(path string, roots []cid.Cid, opts ...Option) (*ReadWrite, error) {
	// TODO support resumption if the path provided contains partially written blocks in v2 format.
	// TODO either lock the file or open exclusively; can we do somethign to reduce edge cases.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, fmt.Errorf("could not open read/write file: %w", err)
	}

	indexcls, ok := index.BuildersByCodec[index.IndexInsertion]
	if !ok {
		return nil, fmt.Errorf("unknownindex  codec: %#v", index.IndexInsertion)
	}
	idx := (indexcls()).(*index.InsertionIndex)

	b := &ReadWrite{
		f:      f,
		idx:    idx,
		header: carv2.NewHeader(0),
	}
	for _, opt := range opts {
		opt(b)
	}
	b.carV1Writer = internalio.NewOffsetWriter(f, int64(b.header.CarV1Offset))
	carV1Reader := internalio.NewOffsetReader(f, int64(b.header.CarV1Offset))
	b.ReadOnly = *ReadOnlyOf(carV1Reader, idx)
	if _, err := f.WriteAt(carv2.Pragma, 0); err != nil {
		return nil, err
	}

	v1Header := &carv1.CarHeader{
		Roots:   roots,
		Version: 1,
	}
	if err := carv1.WriteHeader(v1Header, b.carV1Writer); err != nil {
		return nil, fmt.Errorf("couldn't write car header: %w", err)
	}
	return b, nil
}

func (b *ReadWrite) panicIfFinalized() {
	if b.header.CarV1Size != 0 {
		panic("must not use a read-write blockstore after finalizing")
	}
}

// Put puts a given block to the underlying datastore
func (b *ReadWrite) Put(blk blocks.Block) error {
	// PutMany already calls panicIfFinalized.
	return b.PutMany([]blocks.Block{blk})
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (b *ReadWrite) PutMany(blks []blocks.Block) error {
	b.panicIfFinalized()

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, bl := range blks {
		n := uint64(b.carV1Writer.Position())
		if err := util.LdWrite(b.carV1Writer, bl.Cid().Bytes(), bl.RawData()); err != nil {
			return err
		}
		b.idx.InsertNoReplace(bl.Cid(), n)
	}
	return nil
}

// Finalize finalizes this blockstore by writing the CAR v2 header, along with flattened index
// for more efficient subsequent read.
// After this call, this blockstore can no longer be used for read or write.
func (b *ReadWrite) Finalize() error {
	b.panicIfFinalized()

	b.mu.Lock()
	defer b.mu.Unlock()

	// TODO check if add index option is set and don't write the index then set index offset to zero.
	// TODO see if folks need to continue reading from a finalized blockstore, if so return ReadOnly blockstore here.
	b.header = b.header.WithCarV1Size(uint64(b.carV1Writer.Position()))
	defer b.f.Close()
	if _, err := b.header.WriteTo(internalio.NewOffsetWriter(b.f, carv2.PragmaSize)); err != nil {
		return err
	}
	// TODO if index not needed don't bother flattening it.
	fi, err := b.idx.Flatten()
	if err != nil {
		return err
	}
	return index.WriteTo(fi, internalio.NewOffsetWriter(b.f, int64(b.header.IndexOffset)))
}

func (b *ReadWrite) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	b.panicIfFinalized()

	return b.ReadOnly.AllKeysChan(ctx)
}

func (b *ReadWrite) Has(key cid.Cid) (bool, error) {
	b.panicIfFinalized()

	return b.ReadOnly.Has(key)
}

func (b *ReadWrite) Get(key cid.Cid) (blocks.Block, error) {
	b.panicIfFinalized()

	return b.ReadOnly.Get(key)
}

func (b *ReadWrite) GetSize(key cid.Cid) (int, error) {
	b.panicIfFinalized()

	return b.ReadOnly.GetSize(key)
}
