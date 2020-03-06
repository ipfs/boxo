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

// Dag is a root/selector combo to put into a car
type Dag struct {
	Root     cid.Cid
	Selector ipld.Node
}

// Block is all information and metadata about a block that is part of a car file
type Block struct {
	BlockCID cid.Cid
	Data     []byte
	Offset   uint64
	Size     uint64
}

// SelectiveCar is a car file based on root + selector combos instead of just
// a single root and complete dag walk
type SelectiveCar struct {
	ctx   context.Context
	dags  []Dag
	store ReadStore
}

// OnCarHeaderFunc is called during traversal when the header is created
type OnCarHeaderFunc func(CarHeader) error

// OnNewCarBlockFunc is called during traveral when a new unique block is encountered
type OnNewCarBlockFunc func(Block) error

// SelectiveCarPrepared is a SelectiveCar that has already been traversed, such that it
// can be written quicker with Dump. It also contains metadata already collection about
// the Car file like size and number of blocks that go into it
type SelectiveCarPrepared struct {
	SelectiveCar
	size   uint64
	header CarHeader
	cids   []cid.Cid
}

// NewSelectiveCar creates a new SelectiveCar for the given car file based
// a block store and set of root+selector pairs
func NewSelectiveCar(ctx context.Context, store ReadStore, dags []Dag) SelectiveCar {
	return SelectiveCar{
		ctx:   ctx,
		store: store,
		dags:  dags,
	}
}

func (sc SelectiveCar) traverse(onCarHeader OnCarHeaderFunc, onNewCarBlock OnNewCarBlockFunc) (uint64, error) {
	traverser := &selectiveCarTraverser{onCarHeader, onNewCarBlock, 0, cid.NewSet(), sc}
	return traverser.traverse()
}

// Prepare traverse a car file and collects data on what is about to be written, but
// does not actually write the file
func (sc SelectiveCar) Prepare() (SelectiveCarPrepared, error) {
	var header CarHeader
	var cids []cid.Cid

	onCarHeader := func(h CarHeader) error {
		header = h
		return nil
	}
	onNewCarBlock := func(block Block) error {
		cids = append(cids, block.BlockCID)
		return nil
	}
	size, err := sc.traverse(onCarHeader, onNewCarBlock)
	if err != nil {
		return SelectiveCarPrepared{}, err
	}
	return SelectiveCarPrepared{sc, size, header, cids}, nil
}

func (sc SelectiveCar) Write(w io.Writer, userOnNewCarBlocks ...OnNewCarBlockFunc) error {
	onCarHeader := func(h CarHeader) error {
		if err := WriteHeader(&h, w); err != nil {
			return fmt.Errorf("failed to write car header: %s", err)
		}
		return nil
	}
	onNewCarBlock := func(block Block) error {
		err := util.LdWrite(w, block.BlockCID.Bytes(), block.Data)
		if err != nil {
			return err
		}
		for _, userOnNewCarBlock := range userOnNewCarBlocks {
			err := userOnNewCarBlock(block)
			if err != nil {
				return err
			}
		}
		return nil
	}
	_, err := sc.traverse(onCarHeader, onNewCarBlock)
	return err
}

// Size returns the total size in bytes of the car file that will be written
func (sc SelectiveCarPrepared) Size() uint64 {
	return sc.size
}

// Header returns the header for the car file that will be written
func (sc SelectiveCarPrepared) Header() CarHeader {
	return sc.header
}

// Cids returns the list of unique block cids that will be written to the car file
func (sc SelectiveCarPrepared) Cids() []cid.Cid {
	return sc.cids
}

// Dump writes the car file as quickly as possible based on information already
// collected
func (sc SelectiveCarPrepared) Dump(w io.Writer) error {
	if err := WriteHeader(&sc.header, w); err != nil {
		return fmt.Errorf("failed to write car header: %s", err)
	}
	for _, c := range sc.cids {
		blk, err := sc.store.Get(c)
		if err != nil {
			return err
		}
		raw := blk.RawData()
		err = util.LdWrite(w, c.Bytes(), raw)
		if err != nil {
			return err
		}
	}
	return nil
}

type selectiveCarTraverser struct {
	onCarHeader   OnCarHeaderFunc
	onNewCarBlock OnNewCarBlockFunc
	offset        uint64
	cidSet        *cid.Set
	sc            SelectiveCar
}

func (sct *selectiveCarTraverser) traverse() (uint64, error) {
	err := sct.traverseHeader()
	if err != nil {
		return 0, err
	}
	err = sct.traverseBlocks()
	if err != nil {
		return 0, err
	}
	return sct.offset, nil
}

func (sct *selectiveCarTraverser) traverseHeader() error {
	roots := make([]cid.Cid, 0, len(sct.sc.dags))
	for _, carDag := range sct.sc.dags {
		roots = append(roots, carDag.Root)
	}

	header := CarHeader{
		Roots:   roots,
		Version: 1,
	}

	size, err := HeaderSize(&header)
	if err != nil {
		return err
	}

	sct.offset += size

	return sct.onCarHeader(header)
}

func (sct *selectiveCarTraverser) loader(lnk ipld.Link, ctx ipld.LinkContext) (io.Reader, error) {
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
		err := sct.onNewCarBlock(Block{
			BlockCID: c,
			Data:     raw,
			Offset:   sct.offset,
			Size:     size,
		})
		if err != nil {
			return nil, err
		}
		sct.offset += size
	}
	return bytes.NewReader(raw), nil
}

func (sct *selectiveCarTraverser) traverseBlocks() error {

	nbc := dagpb.AddDagPBSupportToChooser(func(ipld.Link, ipld.LinkContext) ipld.NodeBuilder {
		return ipldfree.NodeBuilder()
	})

	for _, carDag := range sct.sc.dags {
		parsed, err := selector.ParseSelector(carDag.Selector)
		if err != nil {
			return err
		}
		lnk := cidlink.Link{Cid: carDag.Root}
		nb := nbc(lnk, ipld.LinkContext{})
		nd, err := lnk.Load(sct.sc.ctx, ipld.LinkContext{}, nb, sct.loader)
		if err != nil {
			return err
		}
		err = traversal.Progress{
			Cfg: &traversal.Config{
				Ctx:                    sct.sc.ctx,
				LinkLoader:             sct.loader,
				LinkNodeBuilderChooser: nbc,
			},
		}.WalkAdv(nd, parsed, func(traversal.Progress, ipld.Node, traversal.VisitReason) error { return nil })
		if err != nil {
			return err
		}
	}
	return nil
}
