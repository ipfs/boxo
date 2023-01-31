// This package implements the unixfs builtin for ipsl.
package unixfs

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/ipsl"
	unixfs_pb "github.com/ipfs/go-libipfs/unixfs/pb"
	merkledag_pb "github.com/ipfs/go-merkledag/pb"
)

// Everything is a Traversal that will match all the unixfs childs blocks, forever.
func Everything() ipsl.Traversal {
	return EverythingNode{boundScope{scope{}, "unixfs"}}
}

func compileEverything(s boundScope) ipsl.NodeCompiler {
	return func(arguments ...ipsl.SomeNode) (ipsl.SomeNode, error) {
		if len(arguments) != 0 {
			return ipsl.SomeNode{}, ipsl.ErrTypeError{Msg: fmt.Sprintf("empty node called with %d arguments, empty does not take arguments", len(arguments))}
		}

		return ipsl.SomeNode{Node: EverythingNode{s}}, nil
	}
}

type EverythingNode struct {
	scope boundScope
}

func (n EverythingNode) Serialize() (ipsl.AstNode, []ipsl.BoundScope, error) {
	return ipsl.AstNode{
		Type: ipsl.SyntaxTypeValueNode,
		Args: []ipsl.AstNode{{
			Type:    ipsl.SyntaxTypeToken,
			Literal: n.scope.Bound() + ".everything",
		}},
	}, []ipsl.BoundScope{n.scope}, nil
}

func (n EverythingNode) SerializeForNetwork() (ipsl.AstNode, []ipsl.BoundScope, error) {
	return n.Serialize()
}

func (n EverythingNode) Traverse(b blocks.Block) ([]ipsl.CidTraversalPair, error) {
	switch codec := b.Cid().Prefix().Codec; codec {
	case cid.Raw:
		return []ipsl.CidTraversalPair{}, nil
	case cid.DagProtobuf:
		var dagpb merkledag_pb.PBNode
		err := proto.Unmarshal(b.RawData(), &dagpb)
		if err != nil {
			return nil, fmt.Errorf("error parsing dagpb node: %w", err)
		}

		{
			// check somewhat sane format
			var unixfs unixfs_pb.Data
			err = proto.Unmarshal(dagpb.Data, &unixfs)
			if err != nil {
				return nil, fmt.Errorf("error parsing unixfs data field: %w", err)
			}

			if unixfs.Type == nil {
				return nil, fmt.Errorf("missing unixfs type")
			}
			switch typ := *unixfs.Type; typ {
			case unixfs_pb.Data_Raw, unixfs_pb.Data_Directory, unixfs_pb.Data_File,
				unixfs_pb.Data_Metadata, unixfs_pb.Data_Symlink, unixfs_pb.Data_HAMTShard:
				// good
			default:
				return nil, fmt.Errorf("unknown unixfs type %d", typ)
			}
		}

		links := dagpb.Links
		r := make([]ipsl.CidTraversalPair, len(links))
		for i, l := range links {
			if l == nil {
				return nil, fmt.Errorf("missing dagpb link at index %d", i)
			}

			linkCid, err := cid.Cast(l.Hash)
			if err != nil {
				return nil, fmt.Errorf("cid decoding issue at dagpb index %d: %w", i, err)
			}

			r[i] = ipsl.CidTraversalPair{Cid: linkCid, Traversal: n}
		}
		return r, nil
	default:
		return nil, fmt.Errorf("unknown codec for unixfs: %d", codec)
	}
}
