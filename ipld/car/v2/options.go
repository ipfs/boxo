package car

import "github.com/multiformats/go-multicodec"

// ReadOptions holds the configured options after applying a number of
// ReadOption funcs.
//
// This type should not be used directly by end users; it's only exposed as a
// side effect of ReadOption.
type ReadOptions struct {
	ZeroLengthSectionAsEOF bool

	BlockstoreUseWholeCIDs bool
}

// ApplyReadOptions applies ropts and returns the resulting ReadOptions.
func ApplyReadOptions(ropts ...ReadOption) ReadOptions {
	var opts ReadOptions
	for _, opt := range ropts {
		opt(&opts)
	}
	return opts
}

// ReadOption describes an option which affects behavior when parsing CAR files.
type ReadOption func(*ReadOptions)

func (ReadOption) readWriteOption() {}

var _ ReadWriteOption = ReadOption(nil)

// WriteOptions holds the configured options after applying a number of
// WriteOption funcs.
//
// This type should not be used directly by end users; it's only exposed as a
// side effect of WriteOption.
type WriteOptions struct {
	DataPadding  uint64
	IndexPadding uint64
	IndexCodec   multicodec.Code

	BlockstoreAllowDuplicatePuts bool
}

// ApplyWriteOptions applies the given ropts and returns the resulting WriteOptions.
func ApplyWriteOptions(ropts ...WriteOption) WriteOptions {
	var opts WriteOptions
	for _, opt := range ropts {
		opt(&opts)
	}
	// Set defaults for zero valued fields.
	if opts.IndexCodec == 0 {
		opts.IndexCodec = multicodec.CarMultihashIndexSorted
	}
	return opts
}

// WriteOption describes an option which affects behavior when encoding CAR files.
type WriteOption func(*WriteOptions)

func (WriteOption) readWriteOption() {}

var _ ReadWriteOption = WriteOption(nil)

// ReadWriteOption is either a ReadOption or a WriteOption.
type ReadWriteOption interface {
	readWriteOption()
}

// ZeroLengthSectionAsEOF is a read option which allows a CARv1 decoder to treat
// a zero-length section as the end of the input CAR file. For example, this can
// be useful to allow "null padding" after a CARv1 without knowing where the
// padding begins.
func ZeroLengthSectionAsEOF(enable bool) ReadOption {
	return func(o *ReadOptions) {
		o.ZeroLengthSectionAsEOF = enable
	}
}

// UseDataPadding is a write option which sets the padding to be added between
// CARv2 header and its data payload on Finalize.
func UseDataPadding(p uint64) WriteOption {
	return func(o *WriteOptions) {
		o.DataPadding = p
	}
}

// UseIndexPadding is a write option which sets the padding between data payload
// and its index on Finalize.
func UseIndexPadding(p uint64) WriteOption {
	return func(o *WriteOptions) {
		o.IndexPadding = p
	}
}

// UseIndexCodec is a write option which sets the codec used for index generation.
func UseIndexCodec(c multicodec.Code) WriteOption {
	return func(o *WriteOptions) {
		o.IndexCodec = c
	}
}
