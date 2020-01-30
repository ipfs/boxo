package car

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	cid "github.com/ipfs/go-cid"
	util "github.com/ipld/go-car/util"
	"github.com/ipld/go-ipld-prime"
	dagpb "github.com/ipld/go-ipld-prime-proto"
	ipldfree "github.com/ipld/go-ipld-prime/impl/free"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

type CarDag struct {
	Root     cid.Cid
	Selector ipld.Node
}

type CarBlock struct {
	BlockCID cid.Cid
	Offset   uint64
	Size     uint64
}

type SelectiveCar struct {
	ctx   context.Context
	dags  []CarDag
	store ReadStore
}

type SelectiveCarResult struct {
	SelectiveCar
	header    CarHeader
	carBlocks []CarBlock
	size      uint64
}

func NewSelectiveCar(ctx context.Context, store ReadStore, dags []CarDag) SelectiveCar {
	return SelectiveCar{
		ctx:   ctx,
		store: store,
		dags:  dags,
	}
}

type selectiveCarTraverser struct {
	offset uint64
	cidSet *cid.Set
	sc     SelectiveCar
}

func (sc SelectiveCar) Traverse() (SelectiveCarResult, error) {
	traverser := &selectiveCarTraverser{0, cid.NewSet(), sc}
	return traverser.traverse()
}

func (sct *selectiveCarTraverser) traverse() (SelectiveCarResult, error) {
	header, err := sct.traverseHeader()
	if err != nil {
		return SelectiveCarResult{}, err
	}
	carBlocks, err := sct.traverseBlocks()
	if err != nil {
		return SelectiveCarResult{}, err
	}
	return SelectiveCarResult{
		sct.sc,
		header,
		carBlocks,
		sct.offset,
	}, nil
}

func (sct *selectiveCarTraverser) traverseHeader() (CarHeader, error) {
	roots := make([]cid.Cid, 0, len(sct.sc.dags))
	for _, carDag := range sct.sc.dags {
		roots = append(roots, carDag.Root)
	}

	header := CarHeader{
		Roots:   roots,
		Version: 1,
	}

	size, err := SizeHeader(&header)
	if err != nil {
		return CarHeader{}, err
	}

	sct.offset += size

	return header, nil
}

func (sct *selectiveCarTraverser) traverseBlocks() ([]CarBlock, error) {
	var carBlocks []CarBlock
	var loader ipld.Loader = func(lnk ipld.Link, ctx ipld.LinkContext) (io.Reader, error) {
		cl, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, errors.New("Incorrect Link Type")
		}
		c := cl.Cid
		blk, err := sct.sc.store.Get(c)
		if err != nil {
			return nil, err
		}
		raw := blk.RawData()
		if !sct.cidSet.Has(c) {
			sct.cidSet.Add(c)
			size := util.LdSize(c.Bytes(), raw)
			carBlocks = append(carBlocks, CarBlock{
				BlockCID: c,
				Offset:   sct.offset,
				Size:     size,
			})
			sct.offset += size
		}
		return bytes.NewReader(raw), nil
	}

	nbc := dagpb.AddDagPBSupportToChooser(func(ipld.Link, ipld.LinkContext) ipld.NodeBuilder {
		return ipldfree.NodeBuilder()
	})

	for _, carDag := range sct.sc.dags {
		parsed, err := selector.ParseSelector(carDag.Selector)
		if err != nil {
			return nil, err
		}
		lnk := cidlink.Link{Cid: carDag.Root}
		nb := nbc(lnk, ipld.LinkContext{})
		nd, err := lnk.Load(sct.sc.ctx, ipld.LinkContext{}, nb, loader)
		if err != nil {
			return nil, err
		}
		err = traversal.Progress{
			Cfg: &traversal.Config{
				Ctx:                    sct.sc.ctx,
				LinkLoader:             loader,
				LinkNodeBuilderChooser: nbc,
			},
		}.WalkAdv(nd, parsed, func(traversal.Progress, ipld.Node, traversal.VisitReason) error { return nil })
		if err != nil {
			return nil, err
		}
	}
	return carBlocks, nil
}

func (sc SelectiveCarResult) Size() uint64 {
	return sc.size
}

func (sc SelectiveCarResult) CarBlocks() []CarBlock {
	return sc.carBlocks
}

func (sc SelectiveCarResult) Write(w io.Writer) error {
	if err := WriteHeader(&sc.header, w); err != nil {
		return fmt.Errorf("failed to write car header: %s", err)
	}
	for _, carBlock := range sc.carBlocks {
		blk, err := sc.store.Get(carBlock.BlockCID)
		if err != nil {
			return err
		}
		raw := blk.RawData()
		err = util.LdWrite(w, carBlock.BlockCID.Bytes(), raw)
		if err != nil {
			return err
		}
	}
	return nil
}
