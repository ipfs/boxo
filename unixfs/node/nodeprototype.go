package unixfsnode

import (
	"context"

	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
)

type prototypeChooser struct {
	lsys     *ipld.LinkSystem
	existing traversal.LinkTargetNodePrototypeChooser
}

// NodeBuilder for UnixFS Nodes -- note: this expects underlying data that
// has the same format as a normal dagpb node -- in fact, it uses the
// exact same builder but then wraps at the end

type _UnixFSNode__Prototype struct {
	ctx  context.Context
	lsys *ipld.LinkSystem
}

func (p _UnixFSNode__Prototype) NewBuilder() ipld.NodeBuilder {
	var nb _UnixFSNode__Builder
	nb.ctx = p.ctx
	nb.lsys = p.lsys
	nb.Reset()
	return &nb
}

type _UnixFSNode__Builder struct {
	ipld.NodeBuilder
	ctx  context.Context
	lsys *ipld.LinkSystem
}

func (nb *_UnixFSNode__Builder) Build() ipld.Node {
	n := nb.NodeBuilder.Build().(dagpb.PBNode)
	un, err := Reify(nb.ctx, n, nb.lsys)
	if err != nil {
		return n
	}
	return un
}

func (nb *_UnixFSNode__Builder) Reset() {
	snb := dagpb.Type.PBNode.NewBuilder()
	*nb = _UnixFSNode__Builder{snb, nb.ctx, nb.lsys}
}

func (pc prototypeChooser) choose(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
	if lnk, ok := lnk.(cidlink.Link); ok && lnk.Cid.Prefix().Codec == 0x70 {
		return _UnixFSNode__Prototype{lnkCtx.Ctx, pc.lsys}, nil
	}
	return pc.existing(lnk, lnkCtx)
}

func AugmentPrototypeChooser(lsys *ipld.LinkSystem, existing traversal.LinkTargetNodePrototypeChooser) traversal.LinkTargetNodePrototypeChooser {
	return prototypeChooser{lsys: lsys, existing: existing}.choose
}
