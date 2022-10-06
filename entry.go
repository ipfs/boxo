package feather

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"

	pb "github.com/Jorropo/go-featheripfs/internal/pb"
	"google.golang.org/protobuf/proto"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipfs/go-verifcid"
	mh "github.com/multiformats/go-multihash"
)

func create[T any](len int) []T {
	return append([]T{}, make([]T, len)...)
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

const gateway = "http://localhost:8080/ipfs/"
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
	curBlock []byte
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
		state:  [][]region{{{low: 0, high: 1<<64 - 1, c: c}}},
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

		// pop current item from the DFS stack
		last := len(d.state) - 1
		todos := d.state[last]
		todo := todos[0]
		todos = todos[1:]
		if len(todos) == 0 {
			d.state[last] = nil // early gc
			d.state = d.state[:last]
		} else {
			d.state[last] = todos
		}

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
				if err == io.EOF {
					// don't show io.EOF in case peeking is too short
					err = io.ErrUnexpectedEOF
				}
				return 0, fmt.Errorf("Peeking at block data for %s verification: %w", cidStringTruncate(c), err)
			}
			_, err = d.buf.Discard(len(data))
			if err != nil {
				return 0, fmt.Errorf("Critical: Discard is supposed to always succeed as long as we don't read less than buffered: %w", err)
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
					return 0, fmt.Errorf("leaf isn't size is incorrect for %s, expected %d; got %d", cidStringTruncate(c), len(data), expectedSize)
				}
			}
			d.curBlock = data
		case cid.DagProtobuf:
			var block pb.PBNode
			err := proto.Unmarshal(data, &block)
			if err != nil {
				return 0, fmt.Errorf("parsing block for %s: %w", cidStringTruncate(c), err)
			}

			if len(block.Data) == 0 {
				return 0, fmt.Errorf("block %s is missing Data field", cidStringTruncate(c))
			}

			var metadata pb.UnixfsData
			err = proto.Unmarshal(block.Data, &metadata)
			if err != nil {
				return 0, fmt.Errorf("parsing metadata for %s: %w", cidStringTruncate(c), err)
			}

			if metadata.Type == nil {
				return 0, fmt.Errorf("missing unixfs node Type for %s", cidStringTruncate(c))
			}
			switch *metadata.Type {
			case pb.UnixfsData_File:
				blocksizes := metadata.Blocksizes
				links := block.Links
				if len(blocksizes) != len(links) {
					return 0, fmt.Errorf("inconsistent sisterlists for %s, %d vs %d", cidStringTruncate(c), len(blocksizes), len(links))
				}

				if todo.rangeKnown {
					if todo.low < uint64(len(metadata.Data)) {
						high := uint64(len(metadata.Data))
						if high > todo.high {
							high = todo.high
						}
						d.curBlock = metadata.Data[todo.low:high]
					}
				} else {
					d.curBlock = metadata.Data
				}

				filesize := uint64(len(metadata.Data))
				if len(blocksizes) != 0 {
					var subRegions []region
					if todo.rangeKnown {
						var regionsInBound int
						for _, bs := range blocksizes {
							if todo.low <= filesize+bs && filesize < todo.high {
								regionsInBound++
							}
							filesize += bs
						}

						subRegions = create[region](regionsInBound)
						var j int
						cursor := uint64(len(metadata.Data))
						for i, bs := range blocksizes {
							if cursor >= todo.high {
								break
							}
							if todo.low <= cursor+bs {
								var low uint64
								if todo.low > cursor {
									low = todo.low - cursor
								}
								high := todo.high - cursor
								if bs < high {
									high = bs
								}

								subCid, err := loadCidFromBytes(links[i].Hash)
								if err != nil {
									return 0, fmt.Errorf("link %d of %s: %w", i, cidStringTruncate(c), err)
								}

								subRegions[j] = region{
									c:          subCid,
									low:        low,
									high:       high,
									rangeKnown: true,
								}
								j++
							}
							cursor += bs
						}
					} else {
						subRegions = create[region](len(blocksizes))
						for i, bs := range blocksizes {
							subCid, err := loadCidFromBytes(links[i].Hash)
							if err != nil {
								return 0, fmt.Errorf("link %d of %s: %w", i, cidStringTruncate(c), err)
							}

							subRegions[i] = region{
								c:          subCid,
								low:        0,
								high:       bs,
								rangeKnown: true,
							}
							filesize += bs
						}
					}
					d.state = append(d.state, subRegions)
				}

				if todo.rangeKnown {
					expectedSize := todo.high - todo.low
					if filesize != expectedSize {
						return 0, fmt.Errorf("inconsistent filesize for %s, expected %d; got %d", cidStringTruncate(c), expectedSize, filesize)
					}
				}
				if metadata.Filesize != nil {
					if *metadata.Filesize != filesize {
						return 0, fmt.Errorf("inconsistent Filesize metadata field for %s, expected %d; got %d", cidStringTruncate(c), filesize, *metadata.Filesize)
					}
				}
			default:
				return 0, fmt.Errorf("unkown unixfs node type for %s: %s", cidStringTruncate(c), metadata.Type.String())
			}

		default:
			return 0, fmt.Errorf("unknown codec type %d for %s; expected Raw or Dag-PB", pref.Codec, cidStringTruncate(c))
		}
	}

	n := copy(b, d.curBlock)
	d.curBlock = d.curBlock[n:]
	if len(d.curBlock) == 0 {
		d.curBlock = nil // early gc
	}

	return n, nil
}
