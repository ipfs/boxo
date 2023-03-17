package blockstore

import (
	"context"
	"fmt"
	"os"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	blocks "github.com/ipfs/go-libipfs/blocks"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/ipld/go-car/v2/internal/carv1/util"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/ipld/go-car/v2/internal/store"
)

var _ blockstore.Blockstore = (*ReadWrite)(nil)

var (
	errFinalized = fmt.Errorf("cannot write in a carv2 blockstore after finalize")
)

// ReadWrite implements a blockstore that stores blocks in CARv2 format.
// Blocks put into the blockstore can be read back once they are successfully written.
// This implementation is preferable for a write-heavy workload.
// The blocks are written immediately on Put and PutAll calls, while the index is stored in memory
// and updated incrementally.
//
// The Finalize function must be called once the putting blocks are finished.
// Upon calling Finalize header is finalized and index is written out.
// Once finalized, all read and write calls to this blockstore will result in errors.
type ReadWrite struct {
	ronly ReadOnly

	f          *os.File
	dataWriter *internalio.OffsetWriteSeeker
	idx        *store.InsertionIndex
	header     carv2.Header

	finalized bool // also protected by ronly.mu

	opts carv2.Options
}

var WriteAsCarV1 = carv2.WriteAsCarV1
var AllowDuplicatePuts = carv2.AllowDuplicatePuts

// OpenReadWrite creates a new ReadWrite at the given path with a provided set of root CIDs and options.
//
// ReadWrite.Finalize must be called once putting and reading blocks are no longer needed.
// Upon calling ReadWrite.Finalize the CARv2 header and index are written out onto the file and the
// backing file is closed. Once finalized, all read and write calls to this blockstore will result
// in errors. Note, ReadWrite.Finalize must be called on an open instance regardless of whether any
// blocks were put or not.
//
// If a file at given path does not exist, the instantiation will write car.Pragma and data payload
// header (i.e. the inner CARv1 header) onto the file before returning.
//
// When the given path already exists, the blockstore will attempt to resume from it.
// On resumption the existing data sections in file are re-indexed, allowing the caller to continue
// putting any remaining blocks without having to re-ingest blocks for which previous ReadWrite.Put
// returned successfully.
//
// Resumption only works on files that were created by a previous instance of a ReadWrite
// blockstore. This means a file created as a result of a successful call to OpenReadWrite can be
// resumed from as long as write operations such as ReadWrite.Put, ReadWrite.PutMany returned
// successfully. On resumption the roots argument and WithDataPadding option must match the
// previous instantiation of ReadWrite blockstore that created the file. More explicitly, the file
// resuming from must:
//  1. start with a complete CARv2 car.Pragma.
//  2. contain a complete CARv1 data header with root CIDs matching the CIDs passed to the
//     constructor, starting at offset optionally padded by WithDataPadding, followed by zero or
//     more complete data sections. If any corrupt data sections are present the resumption will fail.
//     Note, if set previously, the blockstore must use the same WithDataPadding option as before,
//     since this option is used to locate the CARv1 data payload.
//
// Note, resumption should be used with WithCidDeduplication, so that blocks that are successfully
// written into the file are not re-written. Unless, the user explicitly wants duplicate blocks.
//
// Resuming from finalized files is allowed. However, resumption will regenerate the index
// regardless by scanning every existing block in file.
func OpenReadWrite(path string, roots []cid.Cid, opts ...carv2.Option) (*ReadWrite, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666) // TODO: Should the user be able to configure FileMode permissions?
	if err != nil {
		return nil, fmt.Errorf("could not open read/write file: %w", err)
	}
	rwbs, err := OpenReadWriteFile(f, roots, opts...)
	if err != nil {
		return nil, err
	}
	// close the file when finalizing
	rwbs.ronly.carv2Closer = rwbs.f
	return rwbs, nil
}

// OpenReadWriteFile is similar as OpenReadWrite but lets you control the file lifecycle.
// You are responsible for closing the given file.
func OpenReadWriteFile(f *os.File, roots []cid.Cid, opts ...carv2.Option) (*ReadWrite, error) {
	stat, err := f.Stat()
	if err != nil {
		// Note, we should not get a an os.ErrNotExist here because the flags used to open file includes os.O_CREATE
		return nil, err
	}
	// Try and resume by default if the file size is non-zero.
	resume := stat.Size() != 0
	// If construction of blockstore fails, make sure to close off the open file.
	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	// Instantiate block store.
	// Set the header fileld before applying options since padding options may modify header.
	rwbs := &ReadWrite{
		f:         f,
		idx:       store.NewInsertionIndex(),
		header:    carv2.NewHeader(0),
		opts:      carv2.ApplyOptions(opts...),
		finalized: false,
	}
	rwbs.ronly.opts = rwbs.opts

	if p := rwbs.opts.DataPadding; p > 0 {
		rwbs.header = rwbs.header.WithDataPadding(p)
	}
	if p := rwbs.opts.IndexPadding; p > 0 {
		rwbs.header = rwbs.header.WithIndexPadding(p)
	}

	offset := int64(rwbs.header.DataOffset)
	if rwbs.opts.WriteAsCarV1 {
		offset = 0
	}
	rwbs.dataWriter = internalio.NewOffsetWriter(rwbs.f, offset)
	var v1r internalio.ReadSeekerAt
	v1r, err = internalio.NewOffsetReadSeeker(rwbs.f, offset)
	if err != nil {
		return nil, err
	}
	rwbs.ronly.backing = v1r
	rwbs.ronly.idx = rwbs.idx

	if resume {
		if err = store.ResumableVersion(f, rwbs.opts.WriteAsCarV1); err != nil {
			return nil, err
		}
		if err = store.Resume(
			f,
			rwbs.ronly.backing,
			rwbs.dataWriter,
			rwbs.idx,
			roots,
			rwbs.header.DataOffset,
			rwbs.opts.WriteAsCarV1,
			rwbs.opts.MaxAllowedHeaderSize,
			rwbs.opts.ZeroLengthSectionAsEOF,
		); err != nil {
			return nil, err
		}
	} else {
		if err = rwbs.initWithRoots(!rwbs.opts.WriteAsCarV1, roots); err != nil {
			return nil, err
		}
	}

	return rwbs, nil
}

func (b *ReadWrite) initWithRoots(v2 bool, roots []cid.Cid) error {
	if v2 {
		if _, err := b.f.WriteAt(carv2.Pragma, 0); err != nil {
			return err
		}
	}
	return carv1.WriteHeader(&carv1.CarHeader{Roots: roots, Version: 1}, b.dataWriter)
}

// Put puts a given block to the underlying datastore
func (b *ReadWrite) Put(ctx context.Context, blk blocks.Block) error {
	// PutMany already checks b.ronly.closed.
	return b.PutMany(ctx, []blocks.Block{blk})
}

// PutMany puts a slice of blocks at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (b *ReadWrite) PutMany(ctx context.Context, blks []blocks.Block) error {
	b.ronly.mu.Lock()
	defer b.ronly.mu.Unlock()

	if b.ronly.closed {
		return errClosed
	}
	if b.finalized {
		return errFinalized
	}

	for _, bl := range blks {
		c := bl.Cid()

		if should, err := store.ShouldPut(
			b.idx,
			c,
			b.opts.MaxIndexCidSize,
			b.opts.StoreIdentityCIDs,
			b.opts.BlockstoreAllowDuplicatePuts,
			b.opts.BlockstoreUseWholeCIDs,
		); err != nil {
			return err
		} else if !should {
			continue
		}

		n := uint64(b.dataWriter.Position())
		if err := util.LdWrite(b.dataWriter, c.Bytes(), bl.RawData()); err != nil {
			return err
		}
		b.idx.InsertNoReplace(c, n)
	}
	return nil
}

// Discard closes this blockstore without finalizing its header and index.
// After this call, the blockstore can no longer be used.
//
// Note that this call may block if any blockstore operations are currently in
// progress, including an AllKeysChan that hasn't been fully consumed or cancelled.
func (b *ReadWrite) Discard() {
	// Same semantics as ReadOnly.Close, including allowing duplicate calls.
	// The only difference is that our method is called Discard,
	// to further clarify that we're not properly finalizing and writing a
	// CARv2 file.
	b.ronly.Close()
}

// Finalize finalizes this blockstore by writing the CARv2 header, along with flattened index
// for more efficient subsequent read.
// This is the equivalent to calling FinalizeReadOnly and Close.
// After this call, the blockstore can no longer be used.
func (b *ReadWrite) Finalize() error {
	b.ronly.mu.Lock()
	defer b.ronly.mu.Unlock()

	for _, err := range []error{b.finalizeReadOnlyWithoutMutex(), b.closeWithoutMutex()} {
		if err != nil {
			return err
		}
	}
	return nil
}

// Finalize finalizes this blockstore by writing the CARv2 header, along with flattened index
// for more efficient subsequent read, but keep it open read-only.
// This call should be complemented later by a call to Close.
func (b *ReadWrite) FinalizeReadOnly() error {
	b.ronly.mu.Lock()
	defer b.ronly.mu.Unlock()

	return b.finalizeReadOnlyWithoutMutex()
}

func (b *ReadWrite) finalizeReadOnlyWithoutMutex() error {
	if b.opts.WriteAsCarV1 {
		// all blocks are already properly written to the CARv1 inner container and there's
		// no additional finalization required at the end of the file for a complete v1
		b.finalized = true
		return nil
	}

	if b.ronly.closed {
		// Allow duplicate Finalize calls, just like Close.
		// Still error, just like ReadOnly.Close; it should be discarded.
		return fmt.Errorf("called Finalize or FinalizeReadOnly on a closed blockstore")
	}
	if b.finalized {
		return fmt.Errorf("called Finalize or FinalizeReadOnly on an already finalized blockstore")
	}

	b.finalized = true

	return store.Finalize(b.f, b.header, b.idx, uint64(b.dataWriter.Position()), b.opts.StoreIdentityCIDs, b.opts.IndexCodec)
}

// Close closes the blockstore.
// After this call, the blockstore can no longer be used.
func (b *ReadWrite) Close() error {
	b.ronly.mu.Lock()
	defer b.ronly.mu.Unlock()

	return b.closeWithoutMutex()
}

func (b *ReadWrite) closeWithoutMutex() error {
	if !b.opts.WriteAsCarV1 && !b.finalized {
		return fmt.Errorf("called Close without FinalizeReadOnly first")
	}
	if b.ronly.closed {
		// Allow duplicate Close calls
		// Still error, just like ReadOnly.Close; it should be discarded.
		return fmt.Errorf("called Close on a closed blockstore")
	}

	if err := b.ronly.closeWithoutMutex(); err != nil {
		return err
	}
	return nil
}

func (b *ReadWrite) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	b.ronly.mu.Lock()
	defer b.ronly.mu.Unlock()

	if b.ronly.closed {
		return nil, errClosed
	}

	out := make(chan cid.Cid)

	go func() {
		defer close(out)
		err := b.idx.ForEachCid(func(c cid.Cid, _ uint64) error {
			if !b.opts.BlockstoreUseWholeCIDs {
				c = cid.NewCidV1(cid.Raw, c.Hash())
			}
			select {
			case out <- c:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
		if err != nil {
			maybeReportError(ctx, err)
		}
	}()

	return out, nil
}

func (b *ReadWrite) Has(ctx context.Context, key cid.Cid) (bool, error) {
	b.ronly.mu.Lock()
	defer b.ronly.mu.Unlock()

	if b.ronly.closed {
		return false, errClosed
	}

	return store.Has(
		b.idx,
		key,
		b.opts.MaxIndexCidSize,
		b.opts.StoreIdentityCIDs,
		b.opts.BlockstoreAllowDuplicatePuts,
		b.opts.BlockstoreUseWholeCIDs,
	)
}

func (b *ReadWrite) Get(ctx context.Context, key cid.Cid) (blocks.Block, error) {
	return b.ronly.Get(ctx, key)
}

func (b *ReadWrite) GetSize(ctx context.Context, key cid.Cid) (int, error) {
	return b.ronly.GetSize(ctx, key)
}

func (b *ReadWrite) DeleteBlock(_ context.Context, _ cid.Cid) error {
	return fmt.Errorf("ReadWrite blockstore does not support deleting blocks")
}

func (b *ReadWrite) HashOnRead(enable bool) {
	b.ronly.HashOnRead(enable)
}

func (b *ReadWrite) Roots() ([]cid.Cid, error) {
	return b.ronly.Roots()
}
