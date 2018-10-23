package car

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	cbor "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"

	util "github.com/ipfs/go-car/util"
)

func init() {
	cbor.RegisterCborType(CarHeader{})
}

type CarHeader struct {
	Roots   []cid.Cid
	Version uint64
}

type carWriter struct {
	ds format.DAGService
	w  io.Writer
}

func WriteCar(ctx context.Context, ds format.DAGService, roots []cid.Cid, w io.Writer) error {
	cw := &carWriter{ds: ds, w: w}

	h := &CarHeader{
		Roots:   roots,
		Version: 1,
	}

	if err := cw.WriteHeader(h); err != nil {
		return fmt.Errorf("failed to write car header: %s", err)
	}

	seen := cid.NewSet()
	for _, r := range roots {
		if err := dag.EnumerateChildren(ctx, cw.enumGetLinks, r, seen.Visit); err != nil {
			return err
		}
	}
	return nil
}

func ReadHeader(br *bufio.Reader) (*CarHeader, error) {
	hb, err := util.LdRead(br)
	if err != nil {
		return nil, err
	}

	var ch CarHeader
	if err := cbor.DecodeInto(hb, &ch); err != nil {
		return nil, err
	}

	return &ch, nil
}

func (cw *carWriter) WriteHeader(h *CarHeader) error {
	hb, err := cbor.DumpObject(h)
	if err != nil {
		return err
	}

	return util.LdWrite(cw.w, hb)
}

func (cw *carWriter) enumGetLinks(ctx context.Context, c cid.Cid) ([]*format.Link, error) {
	nd, err := cw.ds.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	if err := cw.writeNode(ctx, nd); err != nil {
		return nil, err
	}

	return nd.Links(), nil
}

func (cw *carWriter) writeNode(ctx context.Context, nd format.Node) error {
	return util.LdWrite(cw.w, nd.Cid().Bytes(), nd.RawData())
}

type carReader struct {
	br     *bufio.Reader
	Header *CarHeader
}

func NewCarReader(r io.Reader) (*carReader, error) {
	br := bufio.NewReader(r)
	ch, err := ReadHeader(br)
	if err != nil {
		return nil, err
	}

	if len(ch.Roots) == 0 {
		return nil, fmt.Errorf("empty car")
	}

	if ch.Version != 1 {
		return nil, fmt.Errorf("invalid car version: %d", ch.Version)
	}

	return &carReader{
		br:     br,
		Header: ch,
	}, nil
}

func (cr *carReader) Next() (blocks.Block, error) {
	c, data, err := util.ReadNode(cr.br)
	if err != nil {
		return nil, err
	}

	hashed, err := c.Prefix().Sum(data)
	if err != nil {
		return nil, err
	}

	if !hashed.Equals(c) {
		return nil, fmt.Errorf("mismatch in content integrity, name: %s, data: %s", c, hashed)
	}

	return blocks.NewBlockWithCid(data, c)
}

func LoadCar(bs bstore.Blockstore, r io.Reader) (*CarHeader, error) {
	cr, err := NewCarReader(r)
	if err != nil {
		return nil, err
	}

	for {
		blk, err := cr.Next()
		switch err {
		case io.EOF:
			return cr.Header, nil
		default:
			return nil, err
		case nil:
		}

		if err := bs.Put(blk); err != nil {
			return nil, err
		}
	}
}
