package blockstore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/multiformats/go-varint"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	carv2 "github.com/ipld/go-car/v2"
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
//
// The Finalize function must be called once the putting blocks are finished.
// Upon calling Finalize header is finalized and index is written out.
// Once finalized, all read and write calls to this blockstore will result in panics.
type ReadWrite struct {
	f           *os.File
	carV1Writer *internalio.OffsetWriteSeeker
	ReadOnly
	idx    *index.InsertionIndex
	header carv2.Header

	dedupCids bool
	resume    bool
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

// WithCidDeduplication makes Put calls ignore blocks if the blockstore already
// has the exact same CID.
// This can help avoid redundancy in a CARv1's list of CID-Block pairs.
//
// Note that this compares whole CIDs, not just multihashes.
func WithCidDeduplication(b *ReadWrite) { // TODO should this take a bool and return an option to allow disabling dedupliation?
	b.dedupCids = true
}

// WithResumption sets whether the blockstore should resume on a file produced by a ReadWrite
// blockstore on which ReadWrite.Finalize was not called.
//
// This option is set to false by default, i.e. disabled. When disabled, the file at path must not
// exist, otherwise an error is returned upon ReadWrite blockstore construction.
//
// When this option is set to true the existing data frames in file are re-indexed, allowing the
// caller to continue putting any remaining blocks without having to re-ingest blocks for which
// previous ReadWrite.Put returned successfully.
//
// Resumption is only allowed on files that satisfy the following criteria:
//   1. start with a complete CAR v2 car.Pragma.
//   2. contain a complete CAR v1 data header with root CIDs matching the CIDs passed to the
//      constructor, starting at offset specified by WithCarV1Padding, followed by zero or more
//      complete data frames. If any corrupt data frames are present the resumption will fail. Note,
//      it is important that new instantiations of ReadWrite blockstore with resumption enabled use
//      the same WithCarV1Padding option, since this option is used to locate the offset at which
//      the data payload starts.
//   3. have not been produced by a ReadWrite blockstore that was finalized, i.e. call to
//      ReadWrite.Finalize returned successfully.
func WithResumption(enabled bool) Option {
	return func(b *ReadWrite) {
		b.resume = enabled
	}
}

// NewReadWrite creates a new ReadWrite at the given path with a provided set of root CIDs and options.
func NewReadWrite(path string, roots []cid.Cid, opts ...Option) (*ReadWrite, error) {
	// TODO either lock the file or open exclusively; can we do somethign to reduce edge cases.

	b := &ReadWrite{
		header: carv2.NewHeader(0),
	}

	indexcls, ok := index.BuildersByCodec[index.IndexInsertion]
	if !ok {
		return nil, fmt.Errorf("unknownindex  codec: %#v", index.IndexInsertion)
	}
	b.idx = (indexcls()).(*index.InsertionIndex)

	for _, opt := range opts {
		opt(b)
	}

	fFlag := os.O_RDWR | os.O_CREATE
	if !b.resume {
		fFlag = fFlag | os.O_EXCL
	}
	var err error
	b.f, err = os.OpenFile(path, fFlag, 0o666) // TODO: Should the user be able to configure FileMode permissions?
	if err != nil {
		return nil, fmt.Errorf("could not open read/write file: %w", err)
	}
	b.carV1Writer = internalio.NewOffsetWriter(b.f, int64(b.header.CarV1Offset))
	v1r := internalio.NewOffsetReadSeeker(b.f, int64(b.header.CarV1Offset))
	b.ReadOnly = ReadOnly{backing: v1r, idx: b.idx, carv2Closer: b.f}

	if b.resume {
		if err = b.resumeWithRoots(roots); err != nil {
			return nil, err
		}
	} else {
		if err = b.initWithRoots(roots); err != nil {
			return nil, err
		}
	}

	return b, nil
}

func (b *ReadWrite) initWithRoots(roots []cid.Cid) error {
	if _, err := b.f.WriteAt(carv2.Pragma, 0); err != nil {
		return err
	}
	return carv1.WriteHeader(&carv1.CarHeader{Roots: roots, Version: 1}, b.carV1Writer)
}

func (b *ReadWrite) resumeWithRoots(roots []cid.Cid) error {
	// On resumption it is expected that the CAR v2 Pragma, and the CAR v1 header is successfully written.
	// Otherwise we cannot resume from the file.
	// Read pragma to assert if b.f is indeed a CAR v2.
	version, err := carv2.ReadVersion(b.f)
	if err != nil {
		// The file is not a valid CAR file and cannot resume from it.
		// Or the write must have failed before pragma was written.
		return err
	}
	if version != 2 {
		// The file is not a CAR v2 and we cannot resume from it.
		return fmt.Errorf("cannot resume on CAR file with version %v", version)
	}

	// Use the given CAR v1 padding to instantiate the CAR v1 reader on file.
	v1r := internalio.NewOffsetReadSeeker(b.ReadOnly.backing, 0)
	header, err := carv1.ReadHeader(bufio.NewReader(v1r))
	if err != nil {
		// Cannot read the CAR v1 header; the file is most likely corrupt.
		return fmt.Errorf("error reading car header: %w", err)
	}
	if !header.Equals(carv1.CarHeader{Roots: roots, Version: 1}) {
		// Cannot resume if version and root does not match.
		return errors.New("cannot resume on file with mismatching data header")
	}

	// TODO See how we can reduce duplicate code here.
	// The code here comes from index.Generate.
	// Copied because we need to populate an insertindex, not a sorted index.
	// Producing a sorted index via generate, then converting it to insertindex is not possible.
	// Because Index interface does not expose internal records.
	// This may be done as part of https://github.com/ipld/go-car/issues/95

	offset, err := carv1.HeaderSize(header)
	if err != nil {
		return err
	}
	frameOffset := int64(0)
	if frameOffset, err = v1r.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}

	for {
		// Grab the length of the frame.
		// Note that ReadUvarint wants a ByteReader.
		length, err := varint.ReadUvarint(v1r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Null padding; treat zero-length frames as an EOF.
		// They don't contain a CID nor block, so they're not useful.
		if length == 0 {
			break // TODO This behaviour should be an option, not default. By default we should error. Hook this up to a write option
		}

		// Grab the CID.
		n, c, err := cid.CidFromReader(v1r)
		if err != nil {
			return err
		}
		b.idx.InsertNoReplace(c, uint64(frameOffset))

		// Seek to the next frame by skipping the block.
		// The frame length includes the CID, so subtract it.
		if frameOffset, err = v1r.Seek(int64(length)-int64(n), io.SeekCurrent); err != nil {
			return err
		}
	}
	// Seek to the end of last skipped block where the writer should resume writing.
	_, err = b.carV1Writer.Seek(frameOffset, io.SeekStart)
	return err
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
		c := bl.Cid()
		if b.dedupCids && b.idx.HasExactCID(c) {
			continue
		}

		n := uint64(b.carV1Writer.Position())
		if err := util.LdWrite(b.carV1Writer, c.Bytes(), bl.RawData()); err != nil {
			return err
		}
		b.idx.InsertNoReplace(c, n)
	}
	return nil
}

// Finalize finalizes this blockstore by writing the CAR v2 header, along with flattened index
// for more efficient subsequent read.
// After this call, this blockstore can no longer be used for read or write.
func (b *ReadWrite) Finalize() error {
	if b.header.CarV1Size != 0 {
		// Allow duplicate Finalize calls, just like Close.
		// Still error, just like ReadOnly.Close; it should be discarded.
		return fmt.Errorf("called Finalize twice")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// TODO check if add index option is set and don't write the index then set index offset to zero.
	// TODO see if folks need to continue reading from a finalized blockstore, if so return ReadOnly blockstore here.
	b.header = b.header.WithCarV1Size(uint64(b.carV1Writer.Position()))
	defer b.Close()
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
