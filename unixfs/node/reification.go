package unixfsnode

import (
	"context"
	"fmt"

	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/directory"
	"github.com/ipfs/go-unixfsnode/hamt"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
)

func asPBNode(maybePBNodeRoot ipld.Node) (dagpb.PBNode, error) {
	if pbNode, ok := maybePBNodeRoot.(dagpb.PBNode); ok {
		return pbNode, nil
	}

	// Shortcut didn't work.  Process via the data model.
	//  The AssignNode method on the pb node already contains all the logic necessary for this, so we use that.
	nb := dagpb.Type.PBNode.NewBuilder()
	if err := nb.AssignNode(maybePBNodeRoot); err != nil {
		return nil, err
	}
	return nb.Build().(dagpb.PBNode), nil
}

// Reify looks at an ipld Node and tries to interpret it as a UnixFSNode
// if successful, it returns the UnixFSNode
func Reify(ctx context.Context, maybePBNodeRoot ipld.Node, lsys *ipld.LinkSystem) (ipld.Node, error) {
	pbNode, err := asPBNode(maybePBNodeRoot)
	if err != nil {
		return nil, fmt.Errorf("unixfsnode.Reify failed: data does not match expected shape for Protobuf Node: %w", err)
	}
	if !pbNode.FieldData().Exists() {
		// no data field, therefore, not UnixFS
		return pbNode, nil
	}
	data, err := data.DecodeUnixFSData(pbNode.Data.Must().Bytes())
	if err != nil {
		// we could not decode the UnixFS data, therefore, not UnixFS
		return pbNode, nil
	}
	builder, ok := reifyFuncs[data.FieldDataType().Int()]
	if !ok {
		return nil, fmt.Errorf("No reification for this UnixFS node type")
	}
	return builder(ctx, pbNode, data, lsys)
}

type reifyTypeFunc func(context.Context, dagpb.PBNode, data.UnixFSData, *ipld.LinkSystem) (ipld.Node, error)

var reifyFuncs = map[int64]reifyTypeFunc{
	data.Data_Directory: directory.NewUnixFSBasicDir,
	data.Data_HAMTShard: hamt.NewUnixFSHAMTShard,
}
