package blockstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/ipld/go-ipld-prime/storage"
	"github.com/multiformats/go-varint"
)

var _ storage.ReadableStorage = (*ReadOnlyStorage)(nil)
var _ storage.StreamingReadableStorage = (*ReadOnlyStorage)(nil)

type ReadOnlyStorage struct {
	ro *ReadOnly
}

func NewReadOnlyStorage(backing io.ReaderAt, idx index.Index, opts ...carv2.Option) (*ReadOnlyStorage, error) {
	ro, err := NewReadOnly(backing, idx, opts...)
	if err != nil {
		return nil, err
	}
	return &ReadOnlyStorage{ro: ro}, nil
}

// OpenReadOnlyStorage opens a read-only blockstore from a CAR file (either v1 or v2), generating an index if it does not exist.
// Note, the generated index if the index does not exist is ephemeral and only stored in memory.
// See car.GenerateIndex and Index.Attach for persisting index onto a CAR file.
func OpenReadOnlyStorage(path string, opts ...carv2.Option) (*ReadOnlyStorage, error) {
	ro, err := OpenReadOnly(path, opts...)
	if err != nil {
		return nil, err
	}
	return &ReadOnlyStorage{ro: ro}, nil
}

func (ros *ReadOnlyStorage) Has(ctx context.Context, keyStr string) (bool, error) {
	// Do the inverse of cid.KeyString(),
	// which is how a valid key for this adapter must've been produced.
	key, err := cidFromBinString(keyStr)
	if err != nil {
		return false, err
	}

	return ros.ro.Has(ctx, key)
}

func (ros *ReadOnlyStorage) Get(ctx context.Context, key string) ([]byte, error) {
	// Do the inverse of cid.KeyString(),
	// which is how a valid key for this adapter must've been produced.
	k, err := cidFromBinString(key)
	if err != nil {
		return nil, err
	}

	// Delegate the Get call.
	block, err := ros.ro.Get(ctx, k)
	if err != nil {
		return nil, err
	}

	// Unwrap the actual raw data for return.
	// Discard the rest.  (It's a shame there was an alloc for that structure.)
	return block.RawData(), nil
}

func (ros *ReadOnlyStorage) GetStream(ctx context.Context, keyStr string) (io.ReadCloser, error) {
	// Do the inverse of cid.KeyString(),
	// which is how a valid key for this adapter must've been produced.
	key, err := cidFromBinString(keyStr)
	if err != nil {
		return nil, err
	}

	// Check if the given CID has multihash.IDENTITY code
	// Note, we do this without locking, since there is no shared information to lock for in order to perform the check.
	if digest, ok, err := isIdentity(key); err != nil {
		return nil, err
	} else if ok {
		return io.NopCloser(bytes.NewReader(digest)), nil
	}

	ros.ro.mu.RLock()
	defer ros.ro.mu.RUnlock()

	if ros.ro.closed {
		return nil, errClosed
	}

	fnSize := -1
	var fnErr error
	var foundOffset int64
	err = ros.ro.idx.GetAll(key, func(offset uint64) bool {
		rdr := internalio.NewOffsetReadSeeker(ros.ro.backing, int64(offset))
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
		if ros.ro.opts.BlockstoreUseWholeCIDs {
			if readCid.Equals(key) {
				fnSize = int(sectionLen) - cidLen
				foundOffset = rdr.Offset()
				return false
			} else {
				return true // continue looking
			}
		} else {
			if bytes.Equal(readCid.Hash(), key.Hash()) {
				fnSize = int(sectionLen) - cidLen
				foundOffset = rdr.Offset()
			}
			return false
		}
	})
	if errors.Is(err, index.ErrNotFound) {
		return nil, blockstore.ErrNotFound
	} else if err != nil {
		return nil, err
	} else if fnErr != nil {
		return nil, fnErr
	}
	if fnSize == -1 {
		return nil, blockstore.ErrNotFound
	}
	return io.NopCloser(io.NewSectionReader(ros.ro.backing, foundOffset, int64(fnSize))), nil
}

func (ros *ReadOnlyStorage) Close() error {
	return ros.ro.Close()
}

func (ros *ReadOnlyStorage) Roots() ([]cid.Cid, error) {
	return ros.ro.Roots()
}

// Do the inverse of cid.KeyString().
// (Unclear why go-cid doesn't offer a function for this itself.)
func cidFromBinString(key string) (cid.Cid, error) {
	l, k, err := cid.CidFromBytes([]byte(key))
	if err != nil {
		return cid.Undef, fmt.Errorf("key was not a cid: %w", err)
	}
	if l != len(key) {
		return cid.Undef, fmt.Errorf("key was not a cid: had %d bytes leftover", len(key)-l)
	}
	return k, nil
}
