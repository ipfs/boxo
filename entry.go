package feather

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipfs/go-verifcid"
	mh "github.com/multiformats/go-multihash"
)

// clone recovers the extra capacity allocated
func clone[T any](s ...T) []T {
	var zeroSlice []T
	return append(zeroSlice, s...)
}

func cidStringTruncate(c cid.Cid) string {
	cidStr := c.String()
	if len(cidStr) > maxCidCharDisplay {
		// please don't use non ASCII bases
		cidStr = cidStr[:maxCidCharDisplay] + "..."
	}
	return cidStr
}

type carHeader struct {
	Roots   []cid.Cid
	Version uint64
}

func init() {
	cbor.RegisterCborType(carHeader{})
}

const gateway = "https://ipfs.io/ipfs/"
const maxHeaderSize = 32 * 1024 * 1024 // 32MiB
const maxBlockSize = 2 * 1024 * 1024   // 2MiB
const maxCidSize = 4096
const maxCidCharDisplay = 512

type region struct {
	c          cid.Cid
	low        uint64
	high       uint64
	rangeKnown bool
}

type downloader struct {
	io.Closer

	buf      bufio.Reader
	state    [][]region
	curBlock int
}

// If DownloadFile returns a non nil error, you MUST call Close on the reader,
// even if reader.Read returns an error.
func DownloadFile(c cid.Cid) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", gateway+c.String(), bytes.NewReader(nil))
	if err != nil {
		return nil, err
	}
	// FIXME: Specify ordered DFS with duplicates
	req.Header.Add("Accept", "application/vnd.ipld.car")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	var good bool
	defer func() {
		if !good {
			resp.Body.Close()
		}
	}()

	r := &downloader{
		Closer: resp.Body,
		state:  clone(clone(region{low: 0, high: 1<<64 - 1, c: c})),
	}
	r.buf = *bufio.NewReaderSize(resp.Body, maxBlockSize*2+4096*2)

	headerSize, err := binary.ReadUvarint(&r.buf)
	if err != nil {
		return nil, err
	}
	if headerSize > maxHeaderSize {
		return nil, fmt.Errorf("header is to big at %d instead of %d", headerSize, maxHeaderSize)
	}

	b := make([]byte, headerSize)
	_, err = io.ReadFull(&r.buf, b)
	if err != nil {
		return nil, err
	}

	h := carHeader{}
	err = cbor.DecodeInto(b, &h)
	if err != nil {
		return nil, err
	}

	const supportedVersion = 1
	if h.Version != supportedVersion {
		return nil, fmt.Errorf("unsupported version %d instead of %d", h.Version, supportedVersion)
	}
	if len(h.Roots) != 1 {
		return nil, fmt.Errorf("header has more roots than expected %d instead of 1", len(h.Roots))
	}
	if h.Roots[0] != c {
		return nil, fmt.Errorf("header root don't match, got %s instead of %s", cidStringTruncate(h.Roots[0]), c.String())
	}

	good = true

	return r, nil
}

func (d *downloader) Read(b []byte) (int, error) {
	if d.curBlock == 0 {
		// have to fill more data in the buffer
		if len(d.state) == 0 {
			// no more data remaining
			return 0, io.EOF
		}
		todos := d.state[len(d.state)-1]
		todo := todos[0]
		var data []byte
		c := todo.c

		pref := c.Prefix()
		switch pref.MhType {
		case mh.IDENTITY:
			data = c.Hash()[1:] // skip the 0x00 prefix
		default:
			if err := verifcid.ValidateCid(c); err != nil {
				return 0, fmt.Errorf("cid %s don't pass safe test: %w", cidStringTruncate(c), err)
			}
			itemLenU, err := binary.ReadUvarint(&d.buf)
			if err != nil {
				return 0, err
			}
			if itemLenU > maxBlockSize+maxCidSize {
				return 0, fmt.Errorf("item size (%d) for %s exceed maxBlockSize+maxCidSize (%d)", itemLenU, cidStringTruncate(c), maxBlockSize+maxCidSize)
			}
			itemLen := int(itemLenU)

			cidLen, cidFound, err := cid.CidFromReader(&d.buf)
			if err != nil {
				return 0, fmt.Errorf("trying to read %s failed to read cid: %w", cidStringTruncate(c), err)
			}
			if cidLen > maxCidSize {
				return 0, fmt.Errorf("cidFound for %s is too big at %d bytes", cidStringTruncate(c), cidLen)
			}
			if cidFound != c {
				return 0, fmt.Errorf("downloading %s but got %s instead", cidStringTruncate(c), cidStringTruncate(cidFound))
			}

			blockSize := itemLen - cidLen
			if blockSize > maxBlockSize {
				return 0, fmt.Errorf("block %s is too big (%d) max %d", cidStringTruncate(c), blockSize, maxBlockSize)
			}
			// TODO: fast path read directly into b if len(b) <= blockSize and type is raw
			data, err = d.buf.Peek(blockSize)
			if err != nil {
				return 0, fmt.Errorf("getting block data for %s for verification: %w", cidStringTruncate(c), err)
			}
			cidGot, err := pref.Sum(data)
			if err != nil {
				return 0, fmt.Errorf("hashing data for %s: %w", cidStringTruncate(c), err)
			}

			if cidGot != c {
				return 0, fmt.Errorf("data integrity failed, expected %s; got %s", cidStringTruncate(c), cidStringTruncate(cidGot))
			}
		}

		switch pref.Codec {
		case cid.Raw:
			if todo.rangeKnown {
				expectedSize := todo.high - todo.low
				if uint64(len(data)) != expectedSize {
					return 0, fmt.Errorf("leaf isn't size is incorrect, expected %d; got %d", len(data), expectedSize)
				}
			}
			d.curBlock = len(data)
		case cid.DagProtobuf:
			return 0, fmt.Errorf("TODO: Unimplemented DagProtobuf")
		default:
			return 0, fmt.Errorf("unknown codec type %d; expected Raw or Dag-PB", pref.Codec)
		}
	}

	toRead := d.curBlock

	if len(b) < toRead {
		toRead = len(b)
	}

	n, err := d.buf.Read(b[:toRead])
	d.curBlock -= n
	return n, err
}
