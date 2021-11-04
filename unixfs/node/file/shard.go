package file

import (
	"context"
	"io"

	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/multiformats/go-multicodec"
)

type shardNodeFile struct {
	ctx       context.Context
	lsys      *ipld.LinkSystem
	substrate ipld.Node
	done      bool
	rdr       io.Reader
}

var _ ipld.Node = (*shardNodeFile)(nil)

func (s *shardNodeFile) Read(p []byte) (int, error) {
	if s.done {
		return 0, io.EOF
	}
	// collect the sub-nodes on first use
	if s.rdr == nil {
		links, err := s.substrate.LookupByString("Links")
		if err != nil {
			return 0, err
		}
		readers := make([]io.Reader, 0)
		lnki := links.ListIterator()
		for !lnki.Done() {
			_, lnk, err := lnki.Next()
			if err != nil {
				return 0, err
			}
			lnkhash, err := lnk.LookupByString("Hash")
			if err != nil {
				return 0, err
			}
			lnklnk, err := lnkhash.AsLink()
			if err != nil {
				return 0, err
			}
			target, err := s.lsys.Load(ipld.LinkContext{Ctx: s.ctx}, lnklnk, protoFor(lnklnk))
			if err != nil {
				return 0, err
			}

			asFSNode, err := NewUnixFSFile(s.ctx, target, s.lsys)
			if err != nil {
				return 0, err
			}
			readers = append(readers, asFSNode)
		}
		s.rdr = io.MultiReader(readers...)
	}
	n, err := s.rdr.Read(p)
	if err == io.EOF {
		s.rdr = nil
		s.done = true
	}
	return n, err
}

func protoFor(link ipld.Link) ipld.NodePrototype {
	if lc, ok := link.(cidlink.Link); ok {
		if lc.Cid.Prefix().Codec == uint64(multicodec.DagPb) {
			return dagpb.Type.PBNode
		}
	}
	return basicnode.Prototype.Any
}

func (s *shardNodeFile) Kind() ipld.Kind {
	return ipld.Kind_Bytes
}

func (s *shardNodeFile) AsBytes() ([]byte, error) {
	return io.ReadAll(s)
}

func (s *shardNodeFile) AsBool() (bool, error) {
	return false, ipld.ErrWrongKind{TypeName: "bool", MethodName: "AsBool", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsInt() (int64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "int", MethodName: "AsInt", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsFloat() (float64, error) {
	return 0, ipld.ErrWrongKind{TypeName: "float", MethodName: "AsFloat", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsString() (string, error) {
	return "", ipld.ErrWrongKind{TypeName: "string", MethodName: "AsString", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsLink() (ipld.Link, error) {
	return nil, ipld.ErrWrongKind{TypeName: "link", MethodName: "AsLink", AppropriateKind: ipld.KindSet_JustBytes}
}

func (s *shardNodeFile) AsNode() (ipld.Node, error) {
	return nil, nil
}

func (s *shardNodeFile) Size() int {
	return 0
}

func (s *shardNodeFile) IsAbsent() bool {
	return false
}

func (s *shardNodeFile) IsNull() bool {
	return s.substrate.IsNull()
}

func (s *shardNodeFile) Length() int64 {
	return 0
}

func (s *shardNodeFile) ListIterator() ipld.ListIterator {
	return nil
}

func (s *shardNodeFile) MapIterator() ipld.MapIterator {
	return nil
}

func (s *shardNodeFile) LookupByIndex(idx int64) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (s *shardNodeFile) LookupByString(key string) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (s *shardNodeFile) LookupByNode(key ipld.Node) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

func (s *shardNodeFile) LookupBySegment(seg ipld.PathSegment) (ipld.Node, error) {
	return nil, ipld.ErrWrongKind{}
}

// shardded files / nodes look like dagpb nodes.
func (s *shardNodeFile) Prototype() ipld.NodePrototype {
	return dagpb.Type.PBNode
}
