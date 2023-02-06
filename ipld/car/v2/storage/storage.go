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
)

var errClosed = errors.New("cannot use a CARv2 storage after closing")

type ReaderWriterAt interface {
	io.ReaderAt
	io.Writer
	io.WriterAt
}

type ReadableCar interface {
	ipldstorage.ReadableStorage
	ipldstorage.StreamingReadableStorage
	Roots() []cid.Cid
}

// WritableCar is compatible with storage.WritableStorage but also returns
// the roots of the CAR. It does not implement ipld.StreamingWritableStorage
// as the CAR format does not support streaming data followed by its CID, so
// any streaming implementation would perform buffering and copy the
// existing storage.PutStream() implementation.
type WritableCar interface {
	ipldstorage.WritableStorage
	Roots() []cid.Cid
	Finalize() error
}

var _ ipldstorage.ReadableStorage = (*StorageCar)(nil)
var _ ipldstorage.StreamingReadableStorage = (*StorageCar)(nil)
var _ ReadableCar = (*StorageCar)(nil)
var _ ipldstorage.WritableStorage = (*StorageCar)(nil)

type StorageCar struct {
	idx        index.Index
	reader     io.ReaderAt
	writer     positionedWriter
	dataWriter *internalio.OffsetWriteSeeker
	header     carv2.Header
	roots      []cid.Cid
	opts       carv2.Options

	closed bool
	mu     sync.RWMutex
}

type positionedWriter interface {
	io.Writer
	Position() int64
}

func NewReadable(reader io.ReaderAt, opts ...carv2.Option) (ReadableCar, error) {
	sc := &StorageCar{opts: carv2.ApplyOptions(opts...)}

	rr := internalio.ToReadSeeker(reader)
	header, err := carv1.ReadHeader(rr, sc.opts.MaxAllowedHeaderSize)
	if err != nil {
		return nil, err
	}
	switch header.Version {
	case 1:
		sc.roots = header.Roots
		sc.reader = reader
		rr.Seek(0, io.SeekStart)
		sc.idx = insertionindex.NewInsertionIndex()
		if err := carv2.LoadIndex(sc.idx, rr, opts...); err != nil {
			return nil, err
		}
	case 2:
		v2r, err := carv2.NewReader(reader, opts...)
		if err != nil {
			return nil, err
		}
		sc.roots, err = v2r.Roots()
		if err != nil {
			return nil, err
		}
		if v2r.Header.HasIndex() {
			ir, err := v2r.IndexReader()
			if err != nil {
				return nil, err
			}
			sc.idx, err = index.ReadFrom(ir)
			if err != nil {
				return nil, err
			}
		} else {
			dr, err := v2r.DataReader()
			if err != nil {
				return nil, err
			}
			sc.idx = insertionindex.NewInsertionIndex()
			if err := carv2.LoadIndex(sc.idx, dr, opts...); err != nil {
				return nil, err
			}
		}
		if sc.reader, err = v2r.DataReader(); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported CAR version: %v", header.Version)
	}

	return sc, nil
}

func NewWritable(writer io.Writer, roots []cid.Cid, opts ...carv2.Option) (WritableCar, error) {
	sc, err := newWritable(writer, roots, opts...)
	if err != nil {
		return nil, err
	}
	return sc.init()
}

func newWritable(writer io.Writer, roots []cid.Cid, opts ...carv2.Option) (*StorageCar, error) {
	sc := &StorageCar{
		writer: &positionTrackingWriter{w: writer},
		idx:    insertionindex.NewInsertionIndex(),
		header: carv2.NewHeader(0),
		opts:   carv2.ApplyOptions(opts...),
		roots:  roots,
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
			return nil, fmt.Errorf("cannot write as CARv2 to a non-seekable writer")
		}
	}

	return sc, nil
}

func newReadableWritable(rw ReaderWriterAt, roots []cid.Cid, opts ...carv2.Option) (*StorageCar, error) {
	sc, err := newWritable(rw, roots, opts...)
	if err != nil {
		return nil, err
	}

	sc.reader = rw
	if !sc.opts.WriteAsCarV1 {
		sc.reader, err = internalio.NewOffsetReadSeeker(rw, int64(sc.header.DataOffset))
		if err != nil {
			return nil, err
		}
	}

	return sc, nil
}

func NewReadableWritable(rw ReaderWriterAt, roots []cid.Cid, opts ...carv2.Option) (*StorageCar, error) {
	sc, err := newReadableWritable(rw, roots, opts...)
	if err != nil {
		return nil, err
	}
	if _, err := sc.init(); err != nil {
		return nil, err
	}
	return sc, nil
}

func OpenReadableWritable(rw ReaderWriterAt, roots []cid.Cid, opts ...carv2.Option) (*StorageCar, error) {
	sc, err := newReadableWritable(rw, roots, opts...)
	if err != nil {
		return nil, err
	}

	// attempt to resume
	rs, err := internalio.NewOffsetReadSeeker(rw, 0)
	if err != nil {
		return nil, err
	}
	if err := store.ResumableVersion(rs, sc.opts.WriteAsCarV1); err != nil {
		return nil, err
	}
	if err := store.Resume(
		rw,
		sc.reader,
		sc.dataWriter,
		sc.idx.(*insertionindex.InsertionIndex),
		roots,
		sc.header.DataOffset,
		sc.opts.WriteAsCarV1,
		sc.opts.MaxAllowedHeaderSize,
		sc.opts.ZeroLengthSectionAsEOF,
	); err != nil {
		return nil, err
	}
	return sc, nil
}

func (sc *StorageCar) init() (WritableCar, error) {
	if !sc.opts.WriteAsCarV1 {
		if _, err := sc.writer.Write(carv2.Pragma); err != nil {
			return nil, err
		}
	}
	var w io.Writer = sc.dataWriter
	if sc.dataWriter == nil {
		w = sc.writer
	}
	if err := carv1.WriteHeader(&carv1.CarHeader{Roots: sc.roots, Version: 1}, w); err != nil {
		return nil, err
	}
	return sc, nil
}

func (sc *StorageCar) Roots() []cid.Cid {
	return sc.roots
}

func (sc *StorageCar) Put(ctx context.Context, keyStr string, data []byte) error {
	keyCid, err := cid.Cast([]byte(keyStr))
	if err != nil {
		return fmt.Errorf("bad CID key: %w", err)
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.closed {
		return errClosed
	}

	idx, ok := sc.idx.(*insertionindex.InsertionIndex)
	if !ok || sc.writer == nil {
		return fmt.Errorf("cannot put into a read-only CAR")
	}

	if should, err := store.ShouldPut(
		idx,
		keyCid,
		sc.opts.MaxIndexCidSize,
		sc.opts.StoreIdentityCIDs,
		sc.opts.BlockstoreAllowDuplicatePuts,
		sc.opts.BlockstoreUseWholeCIDs,
	); err != nil {
		return err
	} else if !should {
		return nil
	}

	w := sc.writer
	if sc.dataWriter != nil {
		w = sc.dataWriter
	}
	n := uint64(w.Position())
	if err := util.LdWrite(w, keyCid.Bytes(), data); err != nil {
		return err
	}
	idx.InsertNoReplace(keyCid, n)

	return nil
}

func (sc *StorageCar) Has(ctx context.Context, keyStr string) (bool, error) {
	keyCid, err := cid.Cast([]byte(keyStr))
	if err != nil {
		return false, fmt.Errorf("bad CID key: %w", err)
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

	_, _, size, err := store.FindCid(
		sc.reader,
		sc.idx,
		keyCid,
		sc.opts.BlockstoreUseWholeCIDs,
		sc.opts.ZeroLengthSectionAsEOF,
		sc.opts.MaxAllowedSectionSize,
		false,
	)
	if errors.Is(err, index.ErrNotFound) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return size > -1, nil
}

func (sc *StorageCar) Get(ctx context.Context, keyStr string) ([]byte, error) {
	rdr, err := sc.GetStream(ctx, keyStr)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(rdr)
}

func (sc *StorageCar) GetStream(ctx context.Context, keyStr string) (io.ReadCloser, error) {
	if sc.reader == nil {
		return nil, fmt.Errorf("cannot read from a write-only CAR")
	}

	keyCid, err := cid.Cast([]byte(keyStr))
	if err != nil {
		return nil, fmt.Errorf("bad CID key: %w", err)
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

	_, offset, size, err := store.FindCid(
		sc.reader,
		sc.idx,
		keyCid,
		sc.opts.BlockstoreUseWholeCIDs,
		sc.opts.ZeroLengthSectionAsEOF,
		sc.opts.MaxAllowedSectionSize,
		false,
	)
	if errors.Is(err, index.ErrNotFound) {
		return nil, ErrNotFound{Cid: keyCid}
	} else if err != nil {
		return nil, err
	}
	return io.NopCloser(io.NewSectionReader(sc.reader, offset, int64(size))), nil
}

func (sc *StorageCar) Finalize() error {
	idx, ok := sc.idx.(*insertionindex.InsertionIndex)
	if !ok || sc.writer == nil {
		// ignore this, it's not writable
		return nil
	}

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
		return fmt.Errorf("called Finalize on a closed storage CAR")
	}

	sc.closed = true

	return store.Finalize(wat, sc.header, idx, uint64(sc.dataWriter.Position()), sc.opts.StoreIdentityCIDs, sc.opts.IndexCodec)
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
