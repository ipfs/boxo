package store

import (
	"io"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	internalio "github.com/ipld/go-car/v2/internal/io"
)

func GenerateIndex(at io.ReaderAt, opts ...carv2.Option) (index.Index, error) {
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
