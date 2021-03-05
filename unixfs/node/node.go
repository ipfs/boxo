package unixfsnode

import (
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/schema"
)

var _ ipld.Node = UnixFSNode(nil)
var _ schema.TypedNode = UnixFSNode(nil)

type UnixFSNode = *_UnixFSNode

type _UnixFSNode struct {
	_substrate dagpb.PBNode
}

func (n UnixFSNode) Kind() ipld.Kind {
	return n._substrate.Kind()
}

// LookupByString looks for the key in the list of links with a matching name
func (n UnixFSNode) LookupByString(key string) (ipld.Node, error) {
	links := n._substrate.FieldLinks()
	link := lookup(links, key)
	if link == nil {
		return nil, schema.ErrNoSuchField{Type: nil /*TODO*/, Field: ipld.PathSegmentOfString(key)}
	}
	return link, nil
}

func (n UnixFSNode) LookupByNode(key ipld.Node) (ipld.Node, error) {
	ks, err := key.AsString()
	if err != nil {
		return nil, err
	}
	return n.LookupByString(ks)
}

func (n UnixFSNode) LookupByIndex(idx int64) (ipld.Node, error) {
	return n._substrate.LookupByIndex(idx)
}

func (n UnixFSNode) LookupBySegment(seg ipld.PathSegment) (ipld.Node, error) {
	return n.LookupByString(seg.String())
}

func (n UnixFSNode) MapIterator() ipld.MapIterator {
	return &UnixFSNode__MapItr{n._substrate.Links.Iterator()}
}

// UnixFSNode map iterator iterates throught the links as if they were a map
// Note: for now it does return links with no name, where the key is just String("")
type UnixFSNode__MapItr struct {
	_substrate *dagpb.PBLinks__Itr
}

func (itr *UnixFSNode__MapItr) Next() (k ipld.Node, v ipld.Node, err error) {
	_, next := itr._substrate.Next()
	if next == nil {
		return nil, nil, ipld.ErrIteratorOverread{}
	}
	if next.FieldName().Exists() {
		return next.FieldName().Must(), &_UnixFSLink{next.FieldHash()}, nil
	}
	nb := dagpb.Type.String.NewBuilder()
	err = nb.AssignString("")
	if err != nil {
		return nil, nil, err
	}
	s := nb.Build()
	return s, next.FieldHash(), nil
}

func (itr *UnixFSNode__MapItr) Done() bool {
	return itr._substrate.Done()
}

// ListIterator returns an iterator which yields key-value pairs
// traversing the node.
// If the node kind is anything other than a list, nil will be returned.
//
// The iterator will yield every entry in the list; that is, it
// can be expected that itr.Next will be called node.Length times
// before itr.Done becomes true.
func (n UnixFSNode) ListIterator() ipld.ListIterator {
	return nil
}

// Length returns the length of a list, or the number of entries in a map,
// or -1 if the node is not of list nor map kind.
func (n UnixFSNode) Length() int64 {
	return n._substrate.FieldLinks().Length()
}

func (n UnixFSNode) IsAbsent() bool {
	return false
}

func (n UnixFSNode) IsNull() bool {
	return false
}

func (n UnixFSNode) AsBool() (bool, error) {
	return n._substrate.AsBool()
}

func (n UnixFSNode) AsInt() (int64, error) {
	return n._substrate.AsInt()
}

func (n UnixFSNode) AsFloat() (float64, error) {
	return n._substrate.AsFloat()
}

func (n UnixFSNode) AsString() (string, error) {
	return n._substrate.AsString()
}

func (n UnixFSNode) AsBytes() ([]byte, error) {
	return n._substrate.AsBytes()
}

func (n UnixFSNode) AsLink() (ipld.Link, error) {
	return n._substrate.AsLink()
}

func (n UnixFSNode) Prototype() ipld.NodePrototype {
	return _UnixFSNode__Prototype{}
}

// satisfy schema.TypedNode
func (UnixFSNode) Type() schema.Type {
	return nil /*TODO:typelit*/
}

func (n UnixFSNode) Representation() ipld.Node {
	return n._substrate.Representation()
}

// Native map accessors

func (n UnixFSNode) Iterator() *UnixFSNode__Itr {

	return &UnixFSNode__Itr{n._substrate.Links.Iterator()}
}

type UnixFSNode__Itr struct {
	_substrate *dagpb.PBLinks__Itr
}

func (itr *UnixFSNode__Itr) Next() (k dagpb.String, v UnixFSLink) {
	_, next := itr._substrate.Next()
	if next == nil {
		return nil, nil
	}
	if next.FieldName().Exists() {
		return next.FieldName().Must(), &_UnixFSLink{next.FieldHash()}
	}
	nb := dagpb.Type.String.NewBuilder()
	err := nb.AssignString("")
	if err != nil {
		return nil, nil
	}
	s := nb.Build()
	return s.(dagpb.String), &_UnixFSLink{next.FieldHash()}
}
func (itr *UnixFSNode__Itr) Done() bool {
	return itr._substrate.Done()
}

func (n UnixFSNode) Lookup(key dagpb.String) UnixFSLink {
	return lookup(n._substrate.FieldLinks(), key.String())
}

// direct access to the links and data

func (n UnixFSNode) FieldLinks() dagpb.PBLinks {
	return n._substrate.FieldLinks()
}

func (n UnixFSNode) FieldData() dagpb.MaybeBytes {
	return n._substrate.FieldData()
}

// we need to lookup by key in a list of dag pb links a fair amount, so just have
// a shortcut method
func lookup(links dagpb.PBLinks, key string) UnixFSLink {
	li := links.Iterator()
	for !li.Done() {
		_, next := li.Next()
		name := ""
		if next.FieldName().Exists() {
			name = next.FieldName().Must().String()
		}
		if key == name {
			return &_UnixFSLink{next.FieldHash()}
		}
	}
	return nil
}
