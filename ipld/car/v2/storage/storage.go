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
	internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/ipld/go-car/v2/internal/store"
	ipldstorage "github.com/ipld/go-ipld-prime/storage"
	"github.com/multiformats/go-varint"
)

var errClosed = fmt.Errorf("cannot use a carv2 storage after closing")

// compatible with the go-ipld-format ErrNotFound, match against
// interface{NotFound() bool}

type ErrNotFound struct {
	Cid cid.Cid
}

func (e ErrNotFound) Error() string {
	if e.Cid == cid.Undef {
		return "ipld: could not find node"
	}
	return "ipld: could not find " + e.Cid.String()
}

func (e ErrNotFound) NotFound() bool {
	return true
}

type ReadableCar interface {
	ipldstorage.ReadableStorage
	ipldstorage.StreamingReadableStorage
	Roots() ([]cid.Cid, error)
}

type WritableCar interface {
	ipldstorage.WritableStorage
	ipldstorage.StreamingWritableStorage
}

var _ ipldstorage.ReadableStorage = (*StorageCar)(nil)
var _ ipldstorage.StreamingReadableStorage = (*StorageCar)(nil)
var _ ReadableCar = (*StorageCar)(nil)

type StorageCar struct {
	idx index.Index
	// iidx   *insertionindex.InsertionIndex
	reader io.ReaderAt
	writer io.Writer
	// header carv2.Header
	opts carv2.Options

	closed bool
	mu     sync.RWMutex
}

func NewReadable(reader io.ReaderAt, idx index.Index, opts ...carv2.Option) (ReadableCar, error) {
	sc := &StorageCar{
		opts: carv2.ApplyOptions(opts...),
	}

	version, err := store.ReadVersion(reader, opts...)
	if err != nil {
		return nil, err
	}
	switch version {
	case 1:
		if idx == nil {
			if idx, err = store.GenerateIndex(reader, opts...); err != nil {
				return nil, err
			}
		}
		sc.reader = reader
		sc.idx = idx
		return sc, nil
	case 2:
		v2r, err := carv2.NewReader(reader, opts...)
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
				if idx, err = store.GenerateIndex(dr, opts...); err != nil {
					return nil, err
				}
			}
		}
		sc.reader, err = v2r.DataReader()
		if err != nil {
			return nil, err
		}
		sc.idx = idx
		return sc, nil
	default:
		return nil, fmt.Errorf("unsupported car version: %v", version)
	}
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

	var fnFound bool
	var fnErr error
	err = sc.idx.GetAll(keyCid, func(offset uint64) bool {
		uar, err := internalio.NewOffsetReadSeeker(sc.reader, int64(offset))
		if err != nil {
			fnErr = err
			return false
		}
		_, err = varint.ReadUvarint(uar)
		if err != nil {
			fnErr = err
			return false
		}
		_, readCid, err := cid.CidFromReader(uar)
		if err != nil {
			fnErr = err
			return false
		}
		if sc.opts.BlockstoreUseWholeCIDs {
			fnFound = readCid.Equals(keyCid)
			return !fnFound // continue looking if we haven't found it
		} else {
			fnFound = bytes.Equal(readCid.Hash(), keyCid.Hash())
			return false
		}
	})
	if errors.Is(err, index.ErrNotFound) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return fnFound, fnErr
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
	var fnErr error
	var foundOffset int64
	err = sc.idx.GetAll(keyCid, func(offset uint64) bool {
		rdr, err := internalio.NewOffsetReadSeeker(sc.reader, int64(offset))
		if err != nil {
			fnErr = err
			return false
		}
		sectionLen, err := varint.ReadUvarint(rdr)
		if err != nil {
			fnErr = err
			return false
		}
		cidLen, readCid, err := cid.CidFromReader(rdr)
		if err != nil {
			fnErr = err
			return false
		}
		if sc.opts.BlockstoreUseWholeCIDs {
			if readCid.Equals(keyCid) {
				fnSize = int(sectionLen) - cidLen
				foundOffset = rdr.(interface{ Offset() int64 }).Offset()
				return false
			} else {
				return true // continue looking
			}
		} else {
			if bytes.Equal(readCid.Hash(), keyCid.Hash()) {
				fnSize = int(sectionLen) - cidLen
				foundOffset = rdr.(interface{ Offset() int64 }).Offset()
			}
			return false
		}
	})
	if errors.Is(err, index.ErrNotFound) {
		return nil, ErrNotFound{Cid: keyCid}
	} else if err != nil {
		return nil, err
	} else if fnErr != nil {
		return nil, fnErr
	}
	if fnSize == -1 {
		return nil, ErrNotFound{Cid: keyCid}
	}
	return io.NopCloser(io.NewSectionReader(sc.reader, foundOffset, int64(fnSize))), nil
}
