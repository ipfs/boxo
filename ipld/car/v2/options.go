package car

// ReadOptions holds the configured options after applying a number of
// ReadOption funcs.
//
// This type should not be used directly by end users; it's only exposed as a
// side effect of ReadOption.
type ReadOptions struct {
	ZeroLegthSectionAsEOF bool

	BlockstoreUseWholeCIDs bool
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
	CarV1Padding uint64
	IndexPadding uint64

	BlockstoreAllowDuplicatePuts bool
}

// WriteOption describes an option which affects behavior when encoding CAR files.
type WriteOption func(*WriteOptions)

func (WriteOption) readWriteOption() {}

var _ ReadWriteOption = WriteOption(nil)

// ReadWriteOption is either a ReadOption or a WriteOption.
type ReadWriteOption interface {
	readWriteOption()
}

// ZeroLegthSectionAsEOF is a read option which allows a CARv1 decoder to treat
// a zero-length section as the end of the input CAR file. For example, this can
// be useful to allow "null padding" after a CARv1 without knowing where the
// padding begins.
func ZeroLegthSectionAsEOF(o *ReadOptions) {
	o.ZeroLegthSectionAsEOF = true
}

// UseCarV1Padding is a write option which sets the padding to be added between
// CAR v2 header and its data payload on Finalize.
func UseCarV1Padding(p uint64) WriteOption {
	return func(o *WriteOptions) {
		o.CarV1Padding = p
	}
}

// UseIndexPadding is a write option which sets the padding between data payload
// and its index on Finalize.
func UseIndexPadding(p uint64) WriteOption {
	return func(o *WriteOptions) {
		o.IndexPadding = p
	}
}
