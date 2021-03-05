package unixfsnode

import (
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
)

// NodeBuilder for UnixFS Nodes -- note: this expects underlying data that
// has the same format as a normal dagpb node -- in fact, it uses the
// exact same builder but then wraps at the end

var Type typeSlab

type typeSlab struct {
	UnixFSNode _UnixFSNode__Prototype
}

type _UnixFSNode__Prototype struct{}

func (_UnixFSNode__Prototype) NewBuilder() ipld.NodeBuilder {
	var nb _UnixFSNode__Builder
	nb.Reset()
	return &nb
}

type _UnixFSNode__Builder struct {
	ipld.NodeBuilder
}

func (nb *_UnixFSNode__Builder) Build() ipld.Node {
	n := nb.NodeBuilder.Build().(dagpb.PBNode)
	return &_UnixFSNode{_substrate: n}
}

func (nb *_UnixFSNode__Builder) Reset() {
	snb := dagpb.Type.PBNode.NewBuilder()
	*nb = _UnixFSNode__Builder{snb}
}
