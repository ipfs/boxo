package blockstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	blockstore "github.com/ipfs/boxo/blockstore"
	carv2 "github.com/ipfs/boxo/ipld/car/v2"
	"github.com/ipfs/boxo/ipld/car/v2/index"
	"github.com/ipfs/boxo/ipld/car/v2/internal/carv1"
	internalio "github.com/ipfs/boxo/ipld/car/v2/internal/io"
	"github.com/ipfs/boxo/ipld/car/v2/internal/store"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-varint"
	"golang.org/x/exp/mmap"
)

var _ blockstore.Blockstore = (*ReadOnly)(nil)

var (
	errZeroLengthSection = fmt.Errorf("zero-length carv2 section not allowed by default; see WithZeroLengthSectionAsEOF option")
	errReadOnly          = fmt.Errorf("called write method on a read-only carv2 blockstore")
	errClosed            = fmt.Errorf("cannot use a carv2 blockstore after closing")
)

// ReadOnly provides a read-only CAR Block Store.
type ReadOnly struct {
	// mu allows ReadWrite to be safe for concurrent use.
	// It's in ReadOnly so that read operations also grab read locks,
	// given that ReadWrite embeds ReadOnly for methods like Get and Has.
	//
	// The main fields guarded by the mutex are the index and the underlying writers.
	// For simplicity, the entirety of the blockstore methods grab the mutex.
	mu sync.RWMutex

	// When true, the blockstore has been closed via Close, Discard, or
	// Finalize, and must not be used. Any further blockstore method calls
	// will return errClosed to avoid panics or broken behavior.
	closed bool

	// The backing containing the data payload in CARv1 format.
	backing io.ReaderAt

	// The CARv1 content index.
	idx index.Index

	// If we called carv2.NewReaderMmap, remember to close it too.
	carv2Closer io.Closer

	opts carv2.Options
}

type contextKey string

const asyncErrHandlerKey contextKey = "asyncErrorHandlerKey"

var UseWholeCIDs = carv2.UseWholeCIDs

// NewReadOnly creates a new ReadOnly blockstore from the backing with a optional index as idx.
// This function accepts both CARv1 and CARv2 backing.
// The blockstore is instantiated with the given index if it is not nil.
//
// Otherwise:
// * For a CARv1 backing an index is generated.
// * For a CARv2 backing an index is only generated if Header.HasIndex returns false.
//
// There is no need to call ReadOnly.Close on instances returned by this function.
func NewReadOnly(backing io.ReaderAt, idx index.Index, opts ...carv2.Option) (*ReadOnly, error) {
	b := &ReadOnly{
		opts: carv2.ApplyOptions(opts...),
	}

	version, err := readVersion(backing, opts...)
	if err != nil {
		return nil, err
	}
	switch version {
	case 1:
		if idx == nil {
			if idx, err = generateIndex(backing, opts...); err != nil {
				return nil, err
			}
		}
		b.backing = backing
		b.idx = idx
		return b, nil
	case 2:
		v2r, err := carv2.NewReader(backing, opts...)
		if err != nil {
			return nil, err
		}
		if idx == nil {
			if v2r.Header.HasIndex() {
				ir, err := v2r.IndexReader()
				if err != nil {
					return nil, err
				}
				idx, err = index.ReadFrom(ir)
				if err != nil {
					return nil, err
				}
			} else {
				dr, err := v2r.DataReader()
				if err != nil {
					return nil, err
				}
				if idx, err = generateIndex(dr, opts...); err != nil {
					return nil, err
				}
			}
		}
		b.backing, err = v2r.DataReader()
		if err != nil {
			return nil, err
		}
		b.idx = idx
		return b, nil
	default:
		return nil, fmt.Errorf("unsupported car version: %v", version)
	}
}

func readVersion(at io.ReaderAt, opts ...carv2.Option) (uint64, error) {
	var rr io.Reader
	switch r := at.(type) {
	case io.Reader:
		rr = r
	default:
		var err error
		rr, err = internalio.NewOffsetReadSeeker(r, 0)
		if err != nil {
			return 0, err
		}
	}
	return carv2.ReadVersion(rr, opts...)
}

func generateIndex(at io.ReaderAt, opts ...carv2.Option) (index.Index, error) {
	var rs io.ReadSeeker
	switch r := at.(type) {
	case io.ReadSeeker:
		rs = r
		// The version may have been read from the given io.ReaderAt; therefore move back to the begining.
		if _, err := rs.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
	default:
		var err error
		rs, err = internalio.NewOffsetReadSeeker(r, 0)
		if err != nil {
			return nil, err
		}
	}

	// Note, we do not set any write options so that all write options fall back onto defaults.
	return carv2.GenerateIndex(rs, opts...)
}

// OpenReadOnly opens a read-only blockstore from a CAR file (either v1 or v2), generating an index if it does not exist.
// Note, the generated index if the index does not exist is ephemeral and only stored in memory.
// See car.GenerateIndex and Index.Attach for persisting index onto a CAR file.
func OpenReadOnly(path string, opts ...carv2.Option) (*ReadOnly, error) {
	f, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}

	robs, err := NewReadOnly(f, nil, opts...)
	if err != nil {
		return nil, err
	}
	robs.carv2Closer = f

	return robs, nil
}

// Index gives direct access to the index.
// You should never add records on your own there.
func (b *ReadOnly) Index() index.Index {
	return b.idx
}

// DeleteBlock is unsupported and always errors.
func (b *ReadOnly) DeleteBlock(_ context.Context, _ cid.Cid) error {
	return errReadOnly
}

// Has indicates if the store contains a block that corresponds to the given key.
// This function always returns true for any given key with multihash.IDENTITY
// code unless the StoreIdentityCIDs option is on, in which case it will defer
// to the index to check for the existence of the block; the index may or may
// not contain identity CIDs included in this CAR, depending on whether
// StoreIdentityCIDs was on when the index was created. If the CAR is a CARv1
// and StoreIdentityCIDs is on, then the index will contain identity CIDs and
// this will always return true.
func (b *ReadOnly) Has(ctx context.Context, key cid.Cid) (bool, error) {
	if !b.opts.StoreIdentityCIDs {
		// If we don't store identity CIDs then we can return them straight away as if they are here,
		// otherwise we need to check for their existence.
		// Note, we do this without locking, since there is no shared information to lock for in order to perform the check.
		if _, ok, err := store.IsIdentity(key); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return false, errClosed
	}

	_, _, size, err := store.FindCid(
		b.backing,
		b.idx,
		key,
		b.opts.BlockstoreUseWholeCIDs,
		b.opts.ZeroLengthSectionAsEOF,
		b.opts.MaxAllowedSectionSize,
		false,
	)
	if errors.Is(err, index.ErrNotFound) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return size > -1, nil
}

// Get gets a block corresponding to the given key.
// This function always returns the block for any given key with
// multihash.IDENTITY code unless the StoreIdentityCIDs option is on, in which
// case it will defer to the index to check for the existence of the block; the
// index may or may not contain identity CIDs included in this CAR, depending on
// whether StoreIdentityCIDs was on when the index was created. If the CAR is a
// CARv1 and StoreIdentityCIDs is on, then the index will contain identity CIDs
// and this will always return true.
func (b *ReadOnly) Get(ctx context.Context, key cid.Cid) (blocks.Block, error) {
	if !b.opts.StoreIdentityCIDs {
		// If we don't store identity CIDs then we can return them straight away as if they are here,
		// otherwise we need to check for their existence.
		// Note, we do this without locking, since there is no shared information to lock for in order to perform the check.
		if digest, ok, err := store.IsIdentity(key); err != nil {
			return nil, err
		} else if ok {
			return blocks.NewBlockWithCid(digest, key)
		}
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return nil, errClosed
	}

	data, _, _, err := store.FindCid(
		b.backing,
		b.idx,
		key,
		b.opts.BlockstoreUseWholeCIDs,
		b.opts.ZeroLengthSectionAsEOF,
		b.opts.MaxAllowedSectionSize,
		true,
	)
	if errors.Is(err, index.ErrNotFound) {
		return nil, format.ErrNotFound{Cid: key}
	} else if err != nil {
		return nil, err
	}
	return blocks.NewBlockWithCid(data, key)
}

// GetSize gets the size of an item corresponding to the given key.
func (b *ReadOnly) GetSize(ctx context.Context, key cid.Cid) (int, error) {
	// Check if the given CID has multihash.IDENTITY code
	// Note, we do this without locking, since there is no shared information to lock for in order to perform the check.
	if digest, ok, err := store.IsIdentity(key); err != nil {
		return 0, err
	} else if ok {
		return len(digest), nil
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return 0, errClosed
	}

	_, _, size, err := store.FindCid(
		b.backing,
		b.idx,
		key,
		b.opts.BlockstoreUseWholeCIDs,
		b.opts.ZeroLengthSectionAsEOF,
		b.opts.MaxAllowedSectionSize,
		false,
	)
	if errors.Is(err, index.ErrNotFound) {
		return -1, format.ErrNotFound{Cid: key}
	} else if err != nil {
		return -1, err
	}
	return size, nil
}

// Put is not supported and always returns an error.
func (b *ReadOnly) Put(context.Context, blocks.Block) error {
	return errReadOnly
}

// PutMany is not supported and always returns an error.
func (b *ReadOnly) PutMany(context.Context, []blocks.Block) error {
	return errReadOnly
}

// WithAsyncErrorHandler returns a context with async error handling set to the given errHandler.
// Any errors that occur during asynchronous operations of AllKeysChan will be passed to the given
// handler.
func WithAsyncErrorHandler(ctx context.Context, errHandler func(error)) context.Context {
	return context.WithValue(ctx, asyncErrHandlerKey, errHandler)
}

// AllKeysChan returns the list of keys in the CAR data payload.
// If the ctx is constructed using WithAsyncErrorHandler any errors that occur during asynchronous
// retrieval of CIDs will be passed to the error handler function set in context.
// Otherwise, errors will terminate the asynchronous operation silently.
//
// See WithAsyncErrorHandler
func (b *ReadOnly) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	// We release the lock when the channel-sending goroutine stops.
	// Note that we can't use a deferred unlock here,
	// because if we return a nil error,
	// we only want to unlock once the async goroutine has stopped.
	b.mu.RLock()

	if b.closed {
		b.mu.RUnlock() // don't hold the mutex forever
		return nil, errClosed
	}

	// TODO we may use this walk for populating the index, and we need to be able to iterate keys in this way somewhere for index generation. In general though, when it's asked for all keys from a blockstore with an index, we should iterate through the index when possible rather than linear reads through the full car.
	rdr, err := internalio.NewOffsetReadSeeker(b.backing, 0)
	if err != nil {
		return nil, err
	}
	header, err := carv1.ReadHeader(rdr, b.opts.MaxAllowedHeaderSize)
	if err != nil {
		b.mu.RUnlock() // don't hold the mutex forever
		return nil, fmt.Errorf("error reading car header: %w", err)
	}
	headerSize, err := carv1.HeaderSize(header)
	if err != nil {
		b.mu.RUnlock() // don't hold the mutex forever
		return nil, err
	}

	// TODO: document this choice of 5, or use simpler buffering like 0 or 1.
	ch := make(chan cid.Cid, 5)

	// Seek to the end of header.
	if _, err = rdr.Seek(int64(headerSize), io.SeekStart); err != nil {
		b.mu.RUnlock() // don't hold the mutex forever
		return nil, err
	}

	go func() {
		defer b.mu.RUnlock()
		defer close(ch)

		for {
			length, err := varint.ReadUvarint(rdr)
			if err != nil {
				if err != io.EOF {
					maybeReportError(ctx, err)
				}
				return
			}

			// Null padding; by default it's an error.
			if length == 0 {
				if b.opts.ZeroLengthSectionAsEOF {
					break
				} else {
					maybeReportError(ctx, errZeroLengthSection)
					return
				}
			}

			thisItemForNxt, err := rdr.Seek(0, io.SeekCurrent)
			if err != nil {
				maybeReportError(ctx, err)
				return
			}
			_, c, err := cid.CidFromReader(rdr)
			if err != nil {
				maybeReportError(ctx, err)
				return
			}
			if _, err := rdr.Seek(thisItemForNxt+int64(length), io.SeekStart); err != nil {
				maybeReportError(ctx, err)
				return
			}

			// If we're just using multihashes, flatten to the "raw" codec.
			if !b.opts.BlockstoreUseWholeCIDs {
				c = cid.NewCidV1(cid.Raw, c.Hash())
			}

			select {
			case ch <- c:
			case <-ctx.Done():
				maybeReportError(ctx, ctx.Err())
				return
			}
		}
	}()
	return ch, nil
}

// maybeReportError checks if an error handler is present in context associated to the key
// asyncErrHandlerKey, and if preset it will pass the error to it.
func maybeReportError(ctx context.Context, err error) {
	value := ctx.Value(asyncErrHandlerKey)
	if eh, _ := value.(func(error)); eh != nil {
		eh(err)
	}
}

// HashOnRead is currently unimplemented; hashing on reads never happens.
func (b *ReadOnly) HashOnRead(bool) {
	// TODO: implement before the final release?
}

// Roots returns the root CIDs of the backing CAR.
func (b *ReadOnly) Roots() ([]cid.Cid, error) {
	ors, err := internalio.NewOffsetReadSeeker(b.backing, 0)
	if err != nil {
		return nil, err
	}
	header, err := carv1.ReadHeader(ors, b.opts.MaxAllowedHeaderSize)
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}
	return header.Roots, nil
}

// Close closes the underlying reader if it was opened by OpenReadOnly.
// After this call, the blockstore can no longer be used.
//
// Note that this call may block if any blockstore operations are currently in
// progress, including an AllKeysChan that hasn't been fully consumed or cancelled.
func (b *ReadOnly) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.closeWithoutMutex()
}

func (b *ReadOnly) closeWithoutMutex() error {
	b.closed = true
	if b.carv2Closer != nil {
		return b.carv2Closer.Close()
	}
	return nil
}
