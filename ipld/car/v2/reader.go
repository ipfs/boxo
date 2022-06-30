package car

import (
	"fmt"
	"io"
	"math"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/ipld/go-car/v2/internal/carv1/util"
	internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
	"golang.org/x/exp/mmap"
)

// Reader represents a reader of CARv2.
type Reader struct {
	Header  Header
	Version uint64
	r       io.ReaderAt
	roots   []cid.Cid
	opts    Options
	closer  io.Closer
}

// OpenReader is a wrapper for NewReader which opens the file at path.
func OpenReader(path string, opts ...Option) (*Reader, error) {
	f, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}

	r, err := NewReader(f, opts...)
	if err != nil {
		return nil, err
	}

	r.closer = f
	return r, nil
}

// NewReader constructs a new reader that reads either CARv1 or CARv2 from the given r.
// Upon instantiation, the reader inspects the payload and provides appropriate read operations
// for both CARv1 and CARv2.
//
// Note that any other version other than 1 or 2 will result in an error. The caller may use
// Reader.Version to get the actual version r represents. In the case where r represents a CARv1
// Reader.Header will not be populated and is left as zero-valued.
func NewReader(r io.ReaderAt, opts ...Option) (*Reader, error) {
	cr := &Reader{
		r: r,
	}
	cr.opts = ApplyOptions(opts...)

	or := internalio.NewOffsetReadSeeker(r, 0)
	var err error
	cr.Version, err = ReadVersion(or, opts...)
	if err != nil {
		return nil, err
	}

	if cr.Version != 1 && cr.Version != 2 {
		return nil, fmt.Errorf("invalid car version: %d", cr.Version)
	}

	if cr.Version == 2 {
		if err := cr.readV2Header(); err != nil {
			return nil, err
		}
	}

	return cr, nil
}

// Roots returns the root CIDs.
// The root CIDs are extracted lazily from the data payload header.
func (r *Reader) Roots() ([]cid.Cid, error) {
	if r.roots != nil {
		return r.roots, nil
	}
	header, err := carv1.ReadHeader(r.DataReader(), r.opts.MaxAllowedHeaderSize)
	if err != nil {
		return nil, err
	}
	r.roots = header.Roots
	return r.roots, nil
}

func (r *Reader) readV2Header() (err error) {
	headerSection := io.NewSectionReader(r.r, PragmaSize, HeaderSize)
	_, err = r.Header.ReadFrom(headerSection)
	return
}

// SectionReader implements both io.ReadSeeker and io.ReaderAt.
// It is the interface version of io.SectionReader, but note that the
// implementation is not guaranteed to be an io.SectionReader.
type SectionReader interface {
	io.Reader
	io.Seeker
	io.ReaderAt
}

// DataReader provides a reader containing the data payload in CARv1 format.
func (r *Reader) DataReader() SectionReader {
	if r.Version == 2 {
		return io.NewSectionReader(r.r, int64(r.Header.DataOffset), int64(r.Header.DataSize))
	}
	return internalio.NewOffsetReadSeeker(r.r, 0)
}

// IndexReader provides an io.Reader containing the index for the data payload if the index is
// present. Otherwise, returns nil.
// Note, this function will always return nil if the backing payload represents a CARv1.
func (r *Reader) IndexReader() io.ReaderAt {
	if r.Version == 1 || !r.Header.HasIndex() {
		return nil
	}
	return internalio.NewOffsetReadSeeker(r.r, int64(r.Header.IndexOffset))
}

// CarStats is returned by an Inspect() call
type CarStats struct {
	Version        uint64
	Header         Header
	Roots          []cid.Cid
	RootsPresent   bool
	BlockCount     uint64
	CodecCounts    map[multicodec.Code]uint64
	MhTypeCounts   map[multicodec.Code]uint64
	AvgCidLength   uint64
	MaxCidLength   uint64
	MinCidLength   uint64
	AvgBlockLength uint64
	MaxBlockLength uint64
	MinBlockLength uint64
	IndexCodec     multicodec.Code
	IndexSize      uint64
}

// Inspect does a quick scan of a CAR, performing basic validation of the format
// and returning a CarStats object that provides a high-level description of the
// contents of the CAR.
// Inspect works for CARv1 and CARv2 contents. A CARv1 will return an
// uninitialized Header value.
// Inspect will perform a basic check of a CARv2 index, where present, but this
// does not guarantee that the index is correct. Attempting to read index data
// from untrusted sources is not recommended. If required, further validation of
// an index can be performed by loading the index and performing a ForEach() and
// sanity checking that the offsets are within the data payload section of the
// CAR. However, re-generation of index data in this case is the recommended
// course of action.
func (r *Reader) Inspect() (CarStats, error) {
	stats := CarStats{
		Version:      r.Version,
		Header:       r.Header,
		CodecCounts:  make(map[multicodec.Code]uint64),
		MhTypeCounts: make(map[multicodec.Code]uint64),
	}

	var totalCidLength uint64
	var totalBlockLength uint64
	var minCidLength uint64 = math.MaxUint64
	var minBlockLength uint64 = math.MaxUint64

	dr := r.DataReader()
	bdr := internalio.ToByteReader(dr)

	// read roots, not using Roots(), because we need the offset setup in the data trader
	header, err := carv1.ReadHeader(dr, r.opts.MaxAllowedHeaderSize)
	if err != nil {
		return CarStats{}, err
	}
	stats.Roots = header.Roots
	var rootsPresentCount int
	rootsPresent := make([]bool, len(stats.Roots))

	// read block sections
	for {
		sectionLength, err := varint.ReadUvarint(bdr)
		if err != nil {
			if err == io.EOF {
				// if the length of bytes read is non-zero when the error is EOF then signal an unclean EOF.
				if sectionLength > 0 {
					return CarStats{}, io.ErrUnexpectedEOF
				}
				// otherwise, this is a normal ending
				break
			}
		} else if sectionLength == 0 && r.opts.ZeroLengthSectionAsEOF {
			// normal ending for this read mode
			break
		}
		if sectionLength > r.opts.MaxAllowedSectionSize {
			return CarStats{}, util.ErrSectionTooLarge
		}

		// decode just the CID bytes
		cidLen, c, err := cid.CidFromReader(dr)
		if err != nil {
			return CarStats{}, err
		}

		// is this a root block? (also account for duplicate root CIDs)
		if rootsPresentCount < len(stats.Roots) {
			for i, r := range stats.Roots {
				if !rootsPresent[i] && c == r {
					rootsPresent[i] = true
					rootsPresentCount++
				}
			}
		}

		cp := c.Prefix()
		codec := multicodec.Code(cp.Codec)
		count := stats.CodecCounts[codec]
		stats.CodecCounts[codec] = count + 1
		mhtype := multicodec.Code(cp.MhType)
		count = stats.MhTypeCounts[mhtype]
		stats.MhTypeCounts[mhtype] = count + 1

		blockLength := sectionLength - uint64(cidLen)
		dr.Seek(int64(blockLength), io.SeekCurrent)

		stats.BlockCount++
		totalCidLength += uint64(cidLen)
		totalBlockLength += blockLength
		if uint64(cidLen) < minCidLength {
			minCidLength = uint64(cidLen)
		}
		if uint64(cidLen) > stats.MaxCidLength {
			stats.MaxCidLength = uint64(cidLen)
		}
		if uint64(blockLength) < minBlockLength {
			minBlockLength = uint64(blockLength)
		}
		if uint64(blockLength) > stats.MaxBlockLength {
			stats.MaxBlockLength = uint64(blockLength)
		}
	}

	stats.RootsPresent = len(stats.Roots) == rootsPresentCount
	if stats.BlockCount > 0 {
		stats.MinCidLength = minCidLength
		stats.MinBlockLength = minBlockLength
		stats.AvgCidLength = totalCidLength / stats.BlockCount
		stats.AvgBlockLength = totalBlockLength / stats.BlockCount
	}

	if stats.Version != 1 && stats.Header.HasIndex() {
		// performs an UnmarshalLazyRead which should have its own validation and
		// is intended to be a fast initial scan
		ind, size, err := index.ReadFromWithSize(r.IndexReader())
		if err != nil {
			return CarStats{}, err
		}
		stats.IndexCodec = ind.Codec()
		stats.IndexSize = uint64(size)
	}

	return stats, nil
}

// Close closes the underlying reader if it was opened by OpenReader.
func (r *Reader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

// ReadVersion reads the version from the pragma.
// This function accepts both CARv1 and CARv2 payloads.
func ReadVersion(r io.Reader, opts ...Option) (uint64, error) {
	o := ApplyOptions(opts...)
	header, err := carv1.ReadHeader(r, o.MaxAllowedHeaderSize)
	if err != nil {
		return 0, err
	}
	return header.Version, nil
}
