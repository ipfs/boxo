package car

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	ipldfree "github.com/ipld/go-ipld-prime/impl/free"
	"github.com/ipld/go-ipld-prime/traversal"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	"github.com/ipld/go-ipld-prime"
	dagpb "github.com/ipld/go-ipld-prime-proto"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"

	util "github.com/ipld/go-car/util"
)

func init() {
	cbor.RegisterCborType(CarHeader{})
}

type Store interface {
	Put(blocks.Block) error
}

type ReadStore interface {
	Get(cid.Cid) (blocks.Block, error)
}

type CarDag struct {
	Root     cid.Cid
	Selector selector.Selector
}

type CarHeader struct {
	Roots   []cid.Cid
	Version uint64
}

type carWriter struct {
	ds   format.DAGService
	w    io.Writer
	walk WalkFunc
}

type WalkFunc func(format.Node) ([]*format.Link, error)

func WriteCar(ctx context.Context, ds format.DAGService, roots []cid.Cid, w io.Writer) error {
	return WriteCarWithWalker(ctx, ds, roots, w, DefaultWalkFunc)
}

func WriteCarWithWalker(ctx context.Context, ds format.DAGService, roots []cid.Cid, w io.Writer, walk WalkFunc) error {


	h := &CarHeader{
		Roots:   roots,
		Version: 1,
	}

	if err := WriteHeader(h, w); err != nil {
		return fmt.Errorf("failed to write car header: %s", err)
	}

	cw := &carWriter{ds: ds, w: w, walk: walk}
	seen := cid.NewSet()
	for _, r := range roots {
		if err := dag.Walk(ctx, cw.enumGetLinks, r, seen.Visit); err != nil {
			return err
		}
	}
	return nil
}

func DefaultWalkFunc(nd format.Node) ([]*format.Link, error) {
	return nd.Links(), nil
}

func WriteSelectiveCar(ctx context.Context, store ReadStore, dags []CarDag, w io.Writer) error {

	roots := make([]cid.Cid, 0, len(dags))
	for _, carDag := range dags {
		roots = append(roots, carDag.Root)
	}

	h := &CarHeader{
		Roots:   roots,
		Version: 1,
	}

	if err := WriteHeader(h, w); err != nil {
		return fmt.Errorf("failed to write car header: %s", err)
	}

	var loader ipld.Loader = func(lnk ipld.Link, ctx ipld.LinkContext) (io.Reader, error) {
		cl, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, errors.New("Incorrect Link Type")
		}
		c := cl.Cid
		fmt.Println(c)
		blk, err := store.Get(c)
		if err != nil {
			return nil, err
		}
		raw := blk.RawData()
		err = util.LdWrite(w, c.Bytes(), raw)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(raw), nil
	}

	nbc := dagpb.AddDagPBSupportToChooser(func(ipld.Link, ipld.LinkContext) ipld.NodeBuilder {
		return ipldfree.NodeBuilder()
	})

	for _, carDag := range dags {
		lnk := cidlink.Link{Cid: carDag.Root}
		nb := nbc(lnk, ipld.LinkContext{})
		nd, err := lnk.Load(ctx, ipld.LinkContext{}, nb, loader)
		if err != nil {
			return err
		}
		err = traversal.Progress{
			Cfg: &traversal.Config{
				Ctx:                    ctx,
				LinkLoader:             loader,
				LinkNodeBuilderChooser: nbc,
			},
		}.WalkAdv(nd, carDag.Selector, func(traversal.Progress, ipld.Node, traversal.VisitReason) error { return nil })
		if err != nil {
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

func WriteHeader(h *CarHeader, w io.Writer) error {
	hb, err := cbor.DumpObject(h)
	if err != nil {
		return err
	}

	return util.LdWrite(w, hb)
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

type CarReader struct {
	br     *bufio.Reader
	Header *CarHeader
}

func NewCarReader(r io.Reader) (*CarReader, error) {
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

	return &CarReader{
		br:     br,
		Header: ch,
	}, nil
}

func (cr *CarReader) Next() (blocks.Block, error) {
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

type batchStore interface {
	PutMany([]blocks.Block) error
}

func LoadCar(s Store, r io.Reader) (*CarHeader, error) {
	cr, err := NewCarReader(r)
	if err != nil {
		return nil, err
	}

	if bs, ok := s.(batchStore); ok {
		return loadCarFast(bs, cr)
	}

	return loadCarSlow(s, cr)
}

func loadCarFast(s batchStore, cr *CarReader) (*CarHeader, error) {
	var buf []blocks.Block
	for {
		blk, err := cr.Next()
		switch err {
		case io.EOF:
			if len(buf) > 0 {
				if err := s.PutMany(buf); err != nil {
					return nil, err
				}
			}
			return cr.Header, nil
		default:
			return nil, err
		case nil:
		}

		buf = append(buf, blk)

		if len(buf) > 1000 {
			if err := s.PutMany(buf); err != nil {
				return nil, err
			}
			buf = buf[:0]
		}
	}
}

func loadCarSlow(s Store, cr *CarReader) (*CarHeader, error) {

	for {
		blk, err := cr.Next()
		switch err {
		case io.EOF:
			return cr.Header, nil
		default:
			return nil, err
		case nil:
		}

		if err := s.Put(blk); err != nil {
			return nil, err
		}
	}
}
