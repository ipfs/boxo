package chunk

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	// DefaultBlockSize is the chunk size that splitters produce (or aim to).
	DefaultBlockSize int64 = 1024 * 256

	// 1 MB, on-wire block size for "datablocks ( unixfs, etc )"
	// copy of https://github.com/ipfs/go-unixfs/blob/v0.2.3/importer/helpers/helpers.go#L8
	BlockSizeLimit int = 1048576

	// in case we are using raw-leaves: this would match BlockSizeLimit, but we can't assume that
	// be conservative and substract the PB wraping size of a full DAG-PB+UnixFS node describing 1M
	// (2b(type2/file)+4b(data-field:3-byte-len-delimited)+4b(size-field:3-byte-varint))+(4b(DAG-type-1:3-byte-len-delimited))
	// FIXME - this calculation will need an update for CBOR
	BlockPayloadLimit int = (BlockSizeLimit - (2 + 4 + 4 + 4))
)

var (
	ErrRabinMin = errors.New("rabin min must be greater than 16")
	ErrSize     = errors.New("chunker size must be greater than 0")
	ErrSizeMax  = fmt.Errorf("chunker parameters may not exceed the maximum block payload size of %d", BlockPayloadLimit)
)

// FromString returns a Splitter depending on the given string:
// it supports "default" (""), "size-{size}", "rabin", "rabin-{blocksize}",
// "rabin-{min}-{avg}-{max}" and "buzhash".
func FromString(r io.Reader, chunker string) (Splitter, error) {
	switch {
	case chunker == "" || chunker == "default":
		return DefaultSplitter(r), nil

	case strings.HasPrefix(chunker, "size-"):
		sizeStr := strings.Split(chunker, "-")[1]
		size, err := strconv.Atoi(sizeStr)
		if err != nil {
			return nil, err
		} else if size <= 0 {
			return nil, ErrSize
		} else if size > BlockPayloadLimit {
			return nil, ErrSizeMax
		}
		return NewSizeSplitter(r, int64(size)), nil

	case strings.HasPrefix(chunker, "rabin"):
		return parseRabinString(r, chunker)

	case chunker == "buzhash":
		return NewBuzhash(r), nil

	default:
		return nil, fmt.Errorf("unrecognized chunker option: %s", chunker)
	}
}

func parseRabinString(r io.Reader, chunker string) (Splitter, error) {
	parts := strings.Split(chunker, "-")
	switch len(parts) {
	case 1:
		return NewRabin(r, uint64(DefaultBlockSize)), nil
	case 2:
		size, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, err
		} else if int(float32(size)*1.5) > BlockPayloadLimit { // FIXME - there is probably a better way to bubble up this calculation from NewRabin()
			return nil, ErrSizeMax
		}
		return NewRabin(r, uint64(size)), nil
	case 4:
		sub := strings.Split(parts[1], ":")
		if len(sub) > 1 && sub[0] != "min" {
			return nil, errors.New("first label must be min")
		}
		min, err := strconv.Atoi(sub[len(sub)-1])
		if err != nil {
			return nil, err
		}
		if min < 16 {
			return nil, ErrRabinMin
		}
		sub = strings.Split(parts[2], ":")
		if len(sub) > 1 && sub[0] != "avg" {
			log.Error("sub == ", sub)
			return nil, errors.New("second label must be avg")
		}
		avg, err := strconv.Atoi(sub[len(sub)-1])
		if err != nil {
			return nil, err
		}

		sub = strings.Split(parts[3], ":")
		if len(sub) > 1 && sub[0] != "max" {
			return nil, errors.New("final label must be max")
		}
		max, err := strconv.Atoi(sub[len(sub)-1])
		if err != nil {
			return nil, err
		}

		if min >= avg {
			return nil, errors.New("incorrect format: rabin-min must be smaller than rabin-avg")
		} else if avg >= max {
			return nil, errors.New("incorrect format: rabin-avg must be smaller than rabin-max")
		} else if max > BlockPayloadLimit {
			return nil, ErrSizeMax
		}

		return NewRabinMinMax(r, uint64(min), uint64(avg), uint64(max)), nil
	default:
		return nil, errors.New("incorrect format (expected 'rabin' 'rabin-[avg]' or 'rabin-[min]-[avg]-[max]'")
	}
}
