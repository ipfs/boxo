package unixfsnode

import (
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/schema"
)

var _ ipld.Node = UnixFSLink(nil)
var _ schema.TypedNode = UnixFSLink(nil)
var _ schema.TypedLinkNode = UnixFSLink(nil)

// UnixFSLink just adds a LinkTargetNodePrototype method to dagpb.Link so that you can cross
// link boundaries correctly in path traversals
type UnixFSLink = *_UnixFSLink

type _UnixFSLink struct {
	dagpb.Link
}

func (n UnixFSLink) LinkTargetNodePrototype() ipld.NodePrototype {
	return _UnixFSNode__Prototype{}
}
