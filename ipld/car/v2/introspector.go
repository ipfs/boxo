package car

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"

	carv1 "github.com/ipld/go-car"
	internalio "github.com/ipld/go-car/v2/internal/io"
)

// Introspection captures the result of an Introspect call.
type Introspection struct {
	carv1.CarHeader
	Header
}

// Introspect introspects the given readable bytes and provides metadata about the characteristics
// and the version of CAR that r represents regardless of its version. This function is backward
// compatible; it supports both CAR v1 and v2.
// Returns error if r does not contain a valid CAR payload.
func Introspect(r io.ReaderAt) (*Introspection, error) {
	i := &Introspection{}
	or := internalio.NewOffsetReader(r, 0)
	header, err := carv1.ReadHeader(bufio.NewReader(or))
	if err != nil {
		return nil, err
	}
	i.CarHeader = *header
	or.SeekOffset(0)
	switch i.Version {
	case 1:
		written, err := io.Copy(ioutil.Discard, or)
		if err != nil {
			return i, err
		}
		i.CarV1Size = uint64(written)
	case 2:
		v2r, err := NewReader(or)
		if err != nil {
			return i, err
		}
		i.Header = v2r.Header
		if i.Roots, err = v2r.Roots(); err != nil {
			return i, err
		}
	default:
		return i, fmt.Errorf("unknown version: %v", i.Version)
	}
	return i, nil
}
