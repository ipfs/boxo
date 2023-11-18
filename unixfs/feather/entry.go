package feather

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"

	"github.com/ipfs/boxo/unixfs"
	"github.com/ipfs/boxo/verifcid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/exp/slices"
)

func cidStringTruncate(c cid.Cid) string {
	cidStr := c.String()
	if len(cidStr) > maxCidCharDisplay {
		// please don't use non ASCII bases
		cidStr = cidStr[:maxCidCharDisplay] + "..."
	}
	return cidStr
}

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
	buf               bufio.Reader
	state             []region
	curBlock          []byte
	readErr           error
	client            *Client
	remainingAttempts uint
	stream            io.Closer
}

type Client struct {
	httpClient *http.Client
	hostname   string
	retries    uint
}

type Option func(*Client) error

// WithHTTPClient allows to use a [http.Client] of your choice.
func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) error {
		c.httpClient = client
		return nil
	}
}

// WithRetries allows to specify how many times we should retry.
// [math.MaxUint] indicate infinite.
func WithRetries(n uint) Option {
	return func(c *Client) error {
		c.retries = n
		return nil
	}
}

// WithStaticGateway sets a static gateway which will be used for all requests.
func WithStaticGateway(gateway string) Option {
	if len(gateway) != 0 && gateway[len(gateway)-1] == '/' {
		gateway = gateway[:len(gateway)-1]
	}
	gateway += "/ipfs/"

	return func(c *Client) error {
		c.hostname = gateway
		return nil
	}
}

var ErrNoAvailableDataSource = errors.New("no data source")

func NewClient(opts ...Option) (*Client, error) {
	c := &Client{
		httpClient: http.DefaultClient,
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	if c.hostname == "" {
		return nil, ErrNoAvailableDataSource
	}

	return c, nil
}

// DownloadFile takes in a [cid.Cid] and return an [io.ReadCloser] which streams the deserialized file.
// You MUST always call the Close method when you are done using it else it would leak resources.
func (client *Client) DownloadFile(c cid.Cid) (io.ReadCloser, error) {
	c = normalizeCidv0(c)

	attempts := client.retries
	if attempts != math.MaxUint {
		attempts++
	}
	d := &downloader{
		client:            client,
		state:             []region{{c: c}},
		buf:               *bufio.NewReaderSize(nil, maxElementSize*2),
		remainingAttempts: attempts,
	}

	return d, nil
}

func (d *downloader) startStream(todo region) error {
	req, err := http.NewRequest("GET", d.client.hostname+todo.c.String()+"?dag-scope=entity", bytes.NewReader(nil))
	if err != nil {
		return err
	}
	req.Header.Add("Accept", "application/vnd.ipld.car;dups=y;order=dfs;version=1")

	resp, err := d.client.httpClient.Do(req)
	if err != nil {
		return err
	}
	var good bool
	defer func() {
		if !good {
			d.Close()
		}
	}()

	d.stream = resp.Body
	d.buf.Reset(resp.Body)

	headerSize, err := binary.ReadUvarint(&d.buf)
	if err != nil {
		return err
	}
	if headerSize > maxHeaderSize {
		return fmt.Errorf("header is to big at %d instead of %d", headerSize, maxHeaderSize)
	}

	_, err = d.buf.Discard(int(headerSize))
	if err != nil {
		return err
	}

	good = true

	return nil
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

func (d *downloader) Read(b []byte) (_ int, err error) {
	if d.readErr != nil {
		return 0, d.readErr
	}
	defer func() {
		d.readErr = err
	}()
	for len(d.curBlock) == 0 {
		// have to fill more data in the buffer
		if len(d.state) == 0 {
			// no more data remaining
			return 0, io.EOF
		}

		// pop current item from the DFS stack
		last := len(d.state) - 1
		todo := d.state[last]
		d.state = d.state[:last]

		var data []byte
		c := todo.c
		c = normalizeCidv0(c)

		pref := c.Prefix()
		switch pref.MhType {
		case mh.IDENTITY:
			data = c.Hash()
			data = data[len(data)-pref.MhLength:] // extract digest
		default:
			if err := verifcid.ValidateCid(verifcid.DefaultAllowlist, c); err != nil {
				return 0, fmt.Errorf("cid %s don't pass safe test: %w", cidStringTruncate(c), err)
			}
			var errStartStream, errRead error
			for {
				if d.stream == nil {
					if attempts := d.remainingAttempts; attempts != math.MaxUint {
						if attempts == 0 {
							return 0, errors.Join(errRead, errStartStream)
						}
						d.remainingAttempts = attempts - 1
					}
					errStartStream = d.startStream(todo)
				}
				data, errRead = d.readBlockFromStream(c)
				if errRead == nil {
					break
				}
				d.stream.Close()
				d.stream = nil
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

			if todo.rangeKnown && todo.size != filesize {
				return 0, fmt.Errorf("inconsistent filesize for %s, expected %d; got %d", cidStringTruncate(c), todo.size, filesize)
			}
		default:
			return 0, fmt.Errorf("unknown unixfs type, got %T for %s", node, cidStringTruncate(c))
		}
	}

	n := copy(b, d.curBlock)
	d.curBlock = d.curBlock[n:]

	return n, nil
}

// readBlockFromStream must perform hash verification on the input.
// The slice returned only has to be valid between two readBlockFromStream and Close calls.
// Implementations should reuse buffers to avoid allocations.
func (d *downloader) readBlockFromStream(expectedCid cid.Cid) ([]byte, error) {
	itemLenU, err := binary.ReadUvarint(&d.buf)
	if err != nil {
		return nil, err
	}
	if itemLenU > maxBlockSize+maxCidSize {
		return nil, fmt.Errorf("item size (%d) for %s exceed maxBlockSize+maxCidSize (%d)", itemLenU, cidStringTruncate(expectedCid), maxBlockSize+maxCidSize)
	}
	itemLen := int(itemLenU)

	cidLen, cidFound, err := cid.CidFromReader(&d.buf)
	if err != nil {
		return nil, fmt.Errorf("trying to read %s failed to read cid: %w", cidStringTruncate(expectedCid), err)
	}
	if cidLen > maxCidSize {
		return nil, fmt.Errorf("cidFound for %s is too big at %d bytes", cidStringTruncate(expectedCid), cidLen)
	}
	cidFound = normalizeCidv0(cidFound)
	if cidFound != expectedCid {
		return nil, fmt.Errorf("downloading %s but got %s instead", cidStringTruncate(expectedCid), cidStringTruncate(cidFound))
	}

	blockSize := itemLen - cidLen
	if blockSize > maxBlockSize {
		return nil, fmt.Errorf("block %s is too big (%d) max %d", cidStringTruncate(expectedCid), blockSize, maxBlockSize)
	}
	data, err := d.buf.Peek(blockSize)
	if err != nil {
		if err == io.EOF {
			// don't show io.EOF in case peeking is too short
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("peeking at block data for %s verification: %w", cidStringTruncate(expectedCid), err)
	}
	_, err = d.buf.Discard(len(data))
	if err != nil {
		return nil, fmt.Errorf("critical: Discard is supposed to always succeed as long as we don't read less than buffered: %w", err)
	}

	cidGot, err := expectedCid.Prefix().Sum(data)
	if err != nil {
		return nil, fmt.Errorf("hashing data for %s: %w", cidStringTruncate(expectedCid), err)
	}
	cidGot = normalizeCidv0(cidGot)

	if cidGot != expectedCid {
		return nil, fmt.Errorf("data integrity failed, expected %s; got %s", cidStringTruncate(expectedCid), cidStringTruncate(cidGot))
	}

	return data, nil
}

func (d *downloader) Close() error {
	if s := d.stream; s != nil {
		d.stream = nil
		return s.Close()
	}
	return nil
}

func normalizeCidv0(c cid.Cid) cid.Cid {
	if c.Version() == 0 {
		return cid.NewCidV1(cid.DagProtobuf, c.Hash())
	}
	return c
}
