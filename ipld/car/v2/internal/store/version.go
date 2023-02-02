package store

import (
	"io"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/internal/carv1"
)

func ReadVersion(at io.ReaderAt, opts ...carv2.Option) (uint64, error) {
	o := carv2.ApplyOptions(opts...)
	header, err := carv1.ReadHeaderAt(at, o.MaxAllowedHeaderSize)
	if err != nil {
		return 0, err
	}
	return header.Version, nil
}
