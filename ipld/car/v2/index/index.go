package index

import (
	"encoding/binary"
	"fmt"
	"io"

	internalio "github.com/ipld/go-car/v2/internal/io"

	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"

	"github.com/multiformats/go-varint"

	"github.com/ipfs/go-cid"
)

type (
	// Record is a pre-processed record of a car item and location.
	Record struct {
		cid.Cid
		Offset uint64
	}

	// Index provides an interface for looking up byte offset of a given CID.
	//
	// Note that each indexing mechanism is free to match CIDs however it
	// sees fit. For example, multicodec.CarIndexSorted only indexes
	// multihash digests, meaning that Get and GetAll will find matching
	// blocks even if the CID's encoding multicodec differs. Other index
	// implementations might index the entire CID, the entire multihash, or
	// just part of a multihash's digest.
	//
	// In accordance with the CARv2 specification, Index will never contain information about CIDs
	// with multihash.IDENTITY code.
	// See: https://ipld.io/specs/transport/car/carv2/#index-format
	Index interface {
		// Codec provides the multicodec code that the index implements.
		//
		// Note that this may return a reserved code if the index
		// implementation is not defined in a spec.
		Codec() multicodec.Code

		// Marshal encodes the index in serial form.
		Marshal(w io.Writer) error
		// Unmarshal decodes the index from its serial form.
		Unmarshal(r io.Reader) error

		// Load inserts a number of records into the index.
		Load([]Record) error

		// GetAll looks up all blocks matching a given CID,
		// calling a function for each one of their offsets.
		//
		// If the function returns false, GetAll stops.
		//
		// If no error occurred and the CID isn't indexed,
		// meaning that no callbacks happen,
		// ErrNotFound is returned.
		GetAll(cid.Cid, func(uint64) bool) error
	}

	// IterableIndex extends Index in cases where the Index is able to
	// provide an iterator for getting the list of all entries in the
	// index.
	IterableIndex interface {
		Index

		// ForEach takes a callback function that will be called
		// on each entry in the index. The arguments to the callback are
		// the multihash of the element, and the offset in the car file
		// where the element appears.
		//
		// If the callback returns a non-nil error, the iteration is aborted,
		// and the ForEach function returns the error to the user.
		ForEach(func(multihash.Multihash, uint64) error) error
	}
)

// GetFirst is a wrapper over Index.GetAll, returning the offset for the first
// matching indexed CID.
func GetFirst(idx Index, key cid.Cid) (uint64, error) {
	var firstOffset uint64
	err := idx.GetAll(key, func(offset uint64) bool {
		firstOffset = offset
		return false
	})
	return firstOffset, err
}

// New constructs a new index corresponding to the given CAR index codec.
func New(codec multicodec.Code) (Index, error) {
	switch codec {
	case multicodec.CarIndexSorted:
		return newSorted(), nil
	case multicodec.CarMultihashIndexSorted:
		return NewMultihashSorted(), nil
	default:
		return nil, fmt.Errorf("unknwon index codec: %v", codec)
	}
}

// WriteTo writes the given idx into w.
// The written bytes include the index encoding.
// This can then be read back using index.ReadFrom
func WriteTo(idx Index, w io.Writer) error {
	buf := make([]byte, binary.MaxVarintLen64)
	b := varint.PutUvarint(buf, uint64(idx.Codec()))
	if _, err := w.Write(buf[:b]); err != nil {
		return err
	}
	return idx.Marshal(w)
}

// ReadFrom reads index from r.
// The reader decodes the index by reading the first byte to interpret the encoding.
// Returns error if the encoding is not known.
func ReadFrom(r io.Reader) (Index, error) {
	code, err := varint.ReadUvarint(internalio.ToByteReader(r))
	if err != nil {
		return nil, err
	}
	codec := multicodec.Code(code)
	idx, err := New(codec)
	if err != nil {
		return nil, err
	}
	if err := idx.Unmarshal(r); err != nil {
		return nil, err
	}
	return idx, nil
}
