package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/ipld/go-car/v2/internal/carv1/util"
	"github.com/ipld/go-car/v2/internal/insertionindex"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/ipld/go-car/v2/internal/store"
	ipldstorage "github.com/ipld/go-ipld-prime/storage"
	"github.com/multiformats/go-varint"
)

var errClosed = fmt.Errorf("cannot use a carv2 storage after closing")

type ReadableCar interface {
	ipldstorage.ReadableStorage
	ipldstorage.StreamingReadableStorage
	Roots() ([]cid.Cid, error)
}

// WritableCar is compatible with storage.WritableStorage but also returns
// the roots of the CAR. It does not implement ipld.StreamingWritableStorage
// as the CAR format does not support streaming data followed by its CID, so
// any streaming implementation would perform buffering and copy the
// existing storage.PutStream() implementation.
type WritableCar interface {
	ipldstorage.WritableStorage
	Roots() ([]cid.Cid, error)
	Finalize() error
}

var _ ipldstorage.ReadableStorage = (*StorageCar)(nil)
var _ ipldstorage.StreamingReadableStorage = (*StorageCar)(nil)
var _ ReadableCar = (*StorageCar)(nil)

type StorageCar struct {
	idx        *insertionindex.InsertionIndex
	reader     io.ReaderAt
	writer     positionedWriter
	dataWriter *internalio.OffsetWriteSeeker
	header     carv2.Header
	opts       carv2.Options

	closed bool
	mu     sync.RWMutex
}

type positionedWriter interface {
	io.Writer
	Position() int64
}

func NewReadable(reader io.ReaderAt, opts ...carv2.Option) (ReadableCar, error) {
	sc := &StorageCar{
		opts: carv2.ApplyOptions(opts...),
		idx:  insertionindex.NewInsertionIndex(),
	}

	rr := internalio.ToReadSeeker(reader)
	header, err := carv1.ReadHeader(rr, sc.opts.MaxAllowedHeaderSize)
	if err != nil {
		return nil, err
	}
	switch header.Version {
	case 1:
		rr.Seek(0, io.SeekStart)
		if err := carv2.LoadIndex(sc.idx, rr, opts...); err != nil {
			return nil, err
		}
		sc.reader = reader
	case 2:
		v2r, err := carv2.NewReader(reader, opts...)
		if err != nil {
			return nil, err
		}
		dr, err := v2r.DataReader()
		if err != nil {
			return nil, err
		}
		if err := carv2.LoadIndex(sc.idx, dr, opts...); err != nil {
			return nil, err
		}
		if sc.reader, err = v2r.DataReader(); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported car version: %v", header.Version)
	}

	return sc, nil
}

func NewWritable(writer io.Writer, roots []cid.Cid, opts ...carv2.Option) (WritableCar, error) {
	sc := &StorageCar{
		writer: &positionTrackingWriter{w: writer},
		idx:    insertionindex.NewInsertionIndex(),
		header: carv2.NewHeader(0),
		opts:   carv2.ApplyOptions(opts...),
	}

	if p := sc.opts.DataPadding; p > 0 {
		sc.header = sc.header.WithDataPadding(p)
	}
	if p := sc.opts.IndexPadding; p > 0 {
		sc.header = sc.header.WithIndexPadding(p)
	}

	offset := int64(sc.header.DataOffset)
	if sc.opts.WriteAsCarV1 {
		offset = 0
	}

	if writerAt, ok := writer.(io.WriterAt); ok {
		sc.dataWriter = internalio.NewOffsetWriter(writerAt, offset)
	} else {
		if !sc.opts.WriteAsCarV1 {
			return nil, fmt.Errorf("cannot write as carv2 to a non-seekable writer")
		}
	}

	if err := sc.initWithRoots(writer, !sc.opts.WriteAsCarV1, roots); err != nil {
		return nil, err
	}

	return sc, nil
}

func (sc *StorageCar) initWithRoots(writer io.Writer, v2 bool, roots []cid.Cid) error {
	if v2 {
		if _, err := writer.Write(carv2.Pragma); err != nil {
			return err
		}
		return carv1.WriteHeader(&carv1.CarHeader{Roots: roots, Version: 1}, sc.dataWriter)
	}
	return carv1.WriteHeader(&carv1.CarHeader{Roots: roots, Version: 1}, writer)
}

func (sc *StorageCar) Put(ctx context.Context, keyStr string, data []byte) error {
	keyCid, err := cid.Cast([]byte(keyStr))
	if err != nil {
		return err
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// If StoreIdentityCIDs option is disabled then treat IDENTITY CIDs like IdStore.
	if !sc.opts.StoreIdentityCIDs {
		// Check for IDENTITY CID. If IDENTITY, ignore and move to the next block.
		if _, ok, err := store.IsIdentity(keyCid); err != nil {
			return err
		} else if ok {
			return nil
		}
	}

	// Check if its size is too big.
	// If larger than maximum allowed size, return error.
	// Note, we need to check this regardless of whether we have IDENTITY CID or not.
	// Since multhihash codes other than IDENTITY can result in large digests.
	cSize := uint64(len(keyCid.Bytes()))
	if cSize > sc.opts.MaxIndexCidSize {
		return &carv2.ErrCidTooLarge{MaxSize: sc.opts.MaxIndexCidSize, CurrentSize: cSize}
	}

	// TODO: if we are write-only and BlockstoreAllowDuplicatePuts then we don't
	// really need an index at all
	if !sc.opts.BlockstoreAllowDuplicatePuts {
		if sc.opts.BlockstoreUseWholeCIDs && sc.idx.HasExactCID(keyCid) {
			return nil // deduplicated by CID
		}
		if !sc.opts.BlockstoreUseWholeCIDs {
			_, err := sc.idx.Get(keyCid)
			if err == nil {
				return nil // deduplicated by hash
			}
		}
	}

	w := sc.writer
	if sc.dataWriter != nil {
		w = sc.dataWriter
	}
	n := uint64(w.Position())
	if err := util.LdWrite(w, keyCid.Bytes(), data); err != nil {
		return err
	}
	sc.idx.InsertNoReplace(keyCid, n)

	return nil
}

func (sc *StorageCar) Roots() ([]cid.Cid, error) {
	ors, err := internalio.NewOffsetReadSeeker(sc.reader, 0)
	if err != nil {
		return nil, err
	}
	header, err := carv1.ReadHeader(ors, sc.opts.MaxAllowedHeaderSize)
	if err != nil {
		return nil, fmt.Errorf("error reading car header: %w", err)
	}
	return header.Roots, nil
}

func (sc *StorageCar) Has(ctx context.Context, keyStr string) (bool, error) {
	keyCid, err := cid.Cast([]byte(keyStr))
	if err != nil {
		return false, err
	}

	if !sc.opts.StoreIdentityCIDs {
		// If we don't store identity CIDs then we can return them straight away as if they are here,
		// otherwise we need to check for their existence.
		// Note, we do this without locking, since there is no shared information to lock for in order to perform the check.
		if _, ok, err := store.IsIdentity(keyCid); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if sc.closed {
		return false, errClosed
	}

	if sc.opts.BlockstoreUseWholeCIDs {
		var foundCid cid.Cid
		_, foundCid, err = sc.idx.GetCid(keyCid)
		if err != nil {
			if !foundCid.Equals(keyCid) {
				return false, nil
			}
		}
	} else {
		_, err = sc.idx.Get(keyCid)
	}
	if errors.Is(err, index.ErrNotFound) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (sc *StorageCar) Get(ctx context.Context, keyStr string) ([]byte, error) {
	rdr, err := sc.GetStream(ctx, keyStr)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(rdr)
}

func (sc *StorageCar) GetStream(ctx context.Context, keyStr string) (io.ReadCloser, error) {
	keyCid, err := cid.Cast([]byte(keyStr))
	if err != nil {
		return nil, err
	}

	if !sc.opts.StoreIdentityCIDs {
		// If we don't store identity CIDs then we can return them straight away as if they are here,
		// otherwise we need to check for their existence.
		// Note, we do this without locking, since there is no shared information to lock for in order to perform the check.
		if digest, ok, err := store.IsIdentity(keyCid); err != nil {
			return nil, err
		} else if ok {
			return io.NopCloser(bytes.NewReader(digest)), nil
		}
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if sc.closed {
		return nil, errClosed
	}

	fnSize := -1
	var offset uint64
	if sc.opts.BlockstoreUseWholeCIDs {
		var foundCid cid.Cid
		offset, foundCid, err = sc.idx.GetCid(keyCid)
		if err != nil {
			if !foundCid.Equals(keyCid) {
				return nil, ErrNotFound{Cid: keyCid}
			}
		}
	} else {
		offset, err = sc.idx.Get(keyCid)
	}
	if errors.Is(err, index.ErrNotFound) {
		return nil, ErrNotFound{Cid: keyCid}
	} else if err != nil {
		return nil, err
	}

	rdr, err := internalio.NewOffsetReadSeeker(sc.reader, int64(offset))
	if err != nil {
		return nil, err
	}
	sectionLen, err := varint.ReadUvarint(rdr)
	if err != nil {
		return nil, err
	}
	cidLen, _, err := cid.CidFromReader(rdr)
	if err != nil {
		return nil, err
	}
	fnSize = int(sectionLen) - cidLen
	offset = uint64(rdr.(interface{ Offset() int64 }).Offset())
	if fnSize == -1 {
		return nil, ErrNotFound{Cid: keyCid}
	}
	return io.NopCloser(io.NewSectionReader(sc.reader, int64(offset), int64(fnSize))), nil
}

func (sc *StorageCar) Finalize() error {
	if sc.opts.WriteAsCarV1 {
		return nil
	}

	wat, ok := sc.writer.(*positionTrackingWriter).w.(io.WriterAt)
	if !ok { // should should already be checked at construction if this is a writable
		return fmt.Errorf("cannot finalize a CARv2 without an io.WriterAt")
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.closed {
		// Allow duplicate Finalize calls, just like Close.
		// Still error, just like ReadOnly.Close; it should be discarded.
		return fmt.Errorf("called Finalize on a closed blockstore")
	}

	// TODO check if add index option is set and don't write the index then set index offset to zero.
	sc.header = sc.header.WithDataSize(uint64(sc.dataWriter.Position()))
	sc.header.Characteristics.SetFullyIndexed(sc.opts.StoreIdentityCIDs)

	sc.closed = true

	fi, err := sc.idx.Flatten(sc.opts.IndexCodec)
	if err != nil {
		return err
	}
	if _, err := index.WriteTo(fi, internalio.NewOffsetWriter(wat, int64(sc.header.IndexOffset))); err != nil {
		return err
	}
	var buf bytes.Buffer
	sc.header.WriteTo(&buf)
	if _, err := sc.header.WriteTo(internalio.NewOffsetWriter(wat, carv2.PragmaSize)); err != nil {
		return err
	}

	return nil
}

type positionTrackingWriter struct {
	w      io.Writer
	offset int64
}

func (ptw *positionTrackingWriter) Write(p []byte) (int, error) {
	written, err := ptw.w.Write(p)
	ptw.offset += int64(written)
	return written, err
}

func (ptw *positionTrackingWriter) Position() int64 {
	return ptw.offset
}
