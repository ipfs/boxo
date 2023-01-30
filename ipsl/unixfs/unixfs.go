// This package implements the unixfs builtin for ipsl.
package unixfs

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/ipsl"
	merkledag_pb "github.com/ipfs/go-merkledag/pb"
	unixfs_pb "github.com/ipfs/go-unixfs/pb"
)

// Everything is a Traversal that will match all the unixfs childs blocks, forever.
func Everything() ipsl.Traversal {
	return EverythingNode{"unixfs"}
}

func compileEverything(scopeName string, arguments ...ipsl.SomeNode) (ipsl.SomeNode, error) {
	if len(arguments) != 0 {
		return ipsl.SomeNode{}, ipsl.ErrTypeError{Msg: fmt.Sprintf("empty node called with %d arguments, empty does not take arguments", len(arguments))}
	}

	return ipsl.SomeNode{Node: EverythingNode{scopeName}}, nil
}

type EverythingNode struct {
	ScopeName string
}

func (n EverythingNode) Serialize() (ipsl.AstNode, error) {
	return ipsl.AstNode{
		Type: ipsl.SyntaxTypeValueNode,
		Args: []ipsl.AstNode{{
			Type:    ipsl.SyntaxTypeToken,
			Literal: n.ScopeName + ".everything",
		}},
	}, nil
}

func (n EverythingNode) SerializeForNetwork() (ipsl.AstNode, error) {
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
