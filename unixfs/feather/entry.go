package feather

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"

	"github.com/ipfs/boxo/unixfs"
	blocks "github.com/ipfs/go-block-format"
	"golang.org/x/exp/slices"

	"github.com/ipfs/boxo/verifcid"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	mh "github.com/multiformats/go-multihash"
)

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

const gateway = "http://localhost:8080/ipfs/"
const maxHeaderSize = 32 * 1024 * 1024 // 32MiB
const maxBlockSize = 2 * 1024 * 1024   // 2MiB
const maxCidSize = 4096
const maxElementSize = maxCidSize + maxBlockSize + binary.MaxVarintLen64
const maxCidCharDisplay = 512

type region struct {
	c          cid.Cid
	size       uint64
	rangeKnown bool
}

type downloader struct {
	io.Closer

	buf      bufio.Reader
	state    []region
	curBlock []byte
}

func DownloadFile(c cid.Cid) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", gateway+c.String()+"?dag-scope=entity", bytes.NewReader(nil))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/vnd.ipld.car;dups=y;order=dfs;version=1")

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
		state:  []region{{c: c}},
	}
	r.buf = *bufio.NewReaderSize(resp.Body, maxElementSize*2)

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

	good = true

	return r, nil
}

func loadCidFromBytes(cidBytes []byte) (cid.Cid, error) {
	if len(cidBytes) == 0 {
		return cid.Cid{}, fmt.Errorf("missing CID")
	}
	if len(cidBytes) > maxCidSize {
		return cid.Cid{}, fmt.Errorf("CID is too big, %d max allowed %d", len(cidBytes), maxCidSize)
	}

	c, err := cid.Cast(cidBytes)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("malphormed CID: %w", err)
	}

	return c, nil
}

func (d *downloader) Read(b []byte) (int, error) {
	for len(d.curBlock) == 0 {
		// have to fill more data in the buffer
		if len(d.state) == 0 {
			// no more data remaining
			return 0, io.EOF
		}

		var good bool
		defer func() {
			if !good {
				d.Close()
			}
		}()

		// pop current item from the DFS stack
		last := len(d.state) - 1
		todo := d.state[last]
		d.state = d.state[:last]

		var data []byte
		c := todo.c

		pref := c.Prefix()
		switch pref.MhType {
		case mh.IDENTITY:
			data = c.Hash()
			data = data[len(data)-pref.MhLength:] // extract digest
		default:
			if err := verifcid.ValidateCid(verifcid.DefaultAllowlist, c); err != nil {
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
				if err == io.EOF {
					// don't show io.EOF in case peeking is too short
					err = io.ErrUnexpectedEOF
				}
				return 0, fmt.Errorf("peeking at block data for %s verification: %w", cidStringTruncate(c), err)
			}
			_, err = d.buf.Discard(len(data))
			if err != nil {
				return 0, fmt.Errorf("critical: Discard is supposed to always succeed as long as we don't read less than buffered: %w", err)
			}

			cidGot, err := pref.Sum(data)
			if err != nil {
				return 0, fmt.Errorf("hashing data for %s: %w", cidStringTruncate(c), err)
			}

			if cidGot != c {
				return 0, fmt.Errorf("data integrity failed, expected %s; got %s", cidStringTruncate(c), cidStringTruncate(cidGot))
			}
		}

		b, err := blocks.NewBlockWithCid(data, c)
		if err != nil {
			return 0, err
		}
		node, err := unixfs.Parse(b)
		if err != nil {
			return 0, err
		}

		switch n := node.(type) {
		case unixfs.File[string, string]:
			d.curBlock = n.Data

			filesize := uint64(len(n.Data))
			if childs := n.Childrens; len(childs) != 0 {
				regions := slices.Grow(d.state, len(childs))
				for i := len(childs); i > 0; {
					i--
					regions = append(regions, region{
						c:          childs[i].Cid,
						size:       childs[i].FileSize,
						rangeKnown: true,
					})
					filesize += childs[i].FileSize
				}
				d.state = regions
			}

			if todo.rangeKnown {
				if todo.size != filesize {
					return 0, fmt.Errorf("inconsistent filesize for %s, expected %d; got %d", cidStringTruncate(c), todo.size, filesize)
				}
			}
		default:
			return 0, fmt.Errorf("unknown unixfs type, got %T for %s", node, cidStringTruncate(c))
		}

		good = true
	}

	n := copy(b, d.curBlock)
	d.curBlock = d.curBlock[n:]
	if len(d.curBlock) == 0 {
		d.curBlock = nil // early gc
	}

	return n, nil
}
