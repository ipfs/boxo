package index

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	internalio "github.com/ipld/go-car/v2/internal/io"

	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
)

// CarIndexNone is a sentinal value used as a multicodec code for the index indicating no index.
const CarIndexNone = 0x300000

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
	// See: multicodec.CarIndexSorted, multicodec.CarMultihashIndexSorted
	Index interface {
		// Codec provides the multicodec code that the index implements.
		//
		// Note that this may return a reserved code if the index
		// implementation is not defined in a spec.
		Codec() multicodec.Code

		// Marshal encodes the index in serial form.
		Marshal(w io.Writer) (uint64, error)

		// Unmarshal decodes the index from its serial form.
		// Deprecated: This function is slurpy and will copy everything into memory.
		Unmarshal(r io.Reader) error

		// UnmarshalLazyRead lazily decodes the index from its serial form. It is a
		// safer alternative to to Unmarshal, particularly when reading index data
		// from untrusted sources (which is not recommended) but also in more
		// constrained memory environments.
		// Instead of slurping UnmarshalLazyRead will keep a reference to the the
		// io.ReaderAt passed in and ask for data as needed.
		UnmarshalLazyRead(r io.ReaderAt) (indexSize int64, err error)

		// Load inserts a number of records into the index.
		// Note that Index will load all given records. Any filtering of the records such as
		// exclusion of CIDs with multihash.IDENTITY code must occur prior to calling this function.
		//
		// Further, the actual information extracted and indexed from the given records entirely
		// depends on the concrete index implementation.
		// For example, some index implementations may only store partial multihashes.
		Load([]Record) error

		// GetAll looks up all blocks matching a given CID,
		// calling a function for each one of their offsets.
		//
		// GetAll stops if the given function returns false,
		// or there are no more offsets; whichever happens first.
		//
		// If no error occurred and the CID isn't indexed,
		// meaning that no callbacks happen,
		// ErrNotFound is returned.
		GetAll(cid.Cid, func(uint64) bool) error

		// ForEach takes a callback function that will be called
		// on each entry in the index. The arguments to the callback are
		// the multihash of the element, and the offset in the car file
		// where the element appears.
		//
		// Note that index with codec multicodec.CarIndexSorted does not support ForEach enumeration.
		// Because this index type only contains the multihash digest and not the code.
		// Calling ForEach on this index type will result in error.
		// Use multicodec.CarMultihashIndexSorted index type instead.
		//
		// If the callback returns a non-nil error, the iteration is aborted,
		// and the ForEach function returns the error to the user.
		//
		// An index may contain multiple offsets corresponding to the same multihash, e.g. via duplicate blocks.
		// In such cases, the given function may be called multiple times with the same multhihash but different offset.
		//
		// The order of calls to the given function is deterministic, but entirely index-specific.
		ForEach(func(multihash.Multihash, uint64) error) error
	}

	// IterableIndex is an index which support iterating over it's elements
	// Deprecated: IterableIndex has been moved into Index. Just use Index now.
	IterableIndex interface {
		Index
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
func WriteTo(idx Index, w io.Writer) (uint64, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	b := varint.PutUvarint(buf, uint64(idx.Codec()))
	n, err := w.Write(buf[:b])
	if err != nil {
		return uint64(n), err
	}

	l, err := idx.Marshal(w)
	return uint64(n) + l, err
}

// ReadFrom reads index from r.
// The reader decodes the index by reading the first byte to interpret the encoding.
// Returns error if the encoding is not known.
// Attempting to read index data from untrusted sources is not recommended.
func ReadFrom(r io.ReaderAt) (Index, error) {
	idx, _, err := ReadFromWithSize(r)
	return idx, err
}

// ReadFromWithSize is just like ReadFrom but return the size of the Index.
// The size is only valid when err != nil.
// Attempting to read index data from untrusted sources is not recommended.
func ReadFromWithSize(r io.ReaderAt) (Index, int64, error) {
	code, err := varint.ReadUvarint(internalio.NewOffsetReadSeeker(r, 0))
	if err != nil {
		return nil, 0, err
	}
	codec := multicodec.Code(code)
	idx, err := New(codec)
	if err != nil {
		return nil, 0, err
	}
	rdr, err := internalio.NewOffsetReadSeekerWithError(r, int64(varint.UvarintSize(code)))
	if err != nil {
		return nil, 0, err
	}
	n, err := idx.UnmarshalLazyRead(rdr)
	if err != nil {
		return nil, 0, err
	}
	return idx, n, nil
}
