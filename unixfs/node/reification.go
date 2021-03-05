package unixfsnode

import (
	"fmt"

	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
)

// Reify looks at an ipld Node and tries to interpret it as a UnixFSNode
// if successful, it returns the UnixFSNode
func Reify(maybePBNodeRoot ipld.Node) (ipld.Node, error) {
	if pbNode, ok := maybePBNodeRoot.(dagpb.PBNode); ok {
		return &_UnixFSNode{_substrate: pbNode}, nil
	}

	// Shortcut didn't work.  Process via the data model.
	//  The AssignNode method on the pb node already contains all the logic necessary for this, so we use that.
	nb := dagpb.Type.PBNode.NewBuilder()
	if err := nb.AssignNode(maybePBNodeRoot); err != nil {
		return nil, fmt.Errorf("unixfsnode.Reify failed: data does not match expected shape for Protobuf Node: %w", err)
	}
	return &_UnixFSNode{nb.Build().(dagpb.PBNode)}, nil

}

// Substrate returns the underlying PBNode -- note: only the substrate will encode successfully to protobuf if writing
func (n UnixFSNode) Substrate() ipld.Node {
	return n._substrate
}
