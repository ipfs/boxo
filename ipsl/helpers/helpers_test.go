package helpers_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/ipsl"
	. "github.com/ipfs/go-libipfs/ipsl/helpers"
	"github.com/multiformats/go-multihash"
	"golang.org/x/exp/slices"
)

type mockTraversal struct {
	t            *testing.T
	expectedCid  cid.Cid
	expectedData []byte
	results      []ipsl.CidTraversalPair
}

func (mockTraversal) Serialize() (ipsl.AstNode, error) {
	panic("Serialize called on mock traversal")
}
func (n mockTraversal) SerializeForNetwork() (ipsl.AstNode, error) {
	return n.Serialize()
}

func (n mockTraversal) Traverse(c cid.Cid, data []byte) ([]ipsl.CidTraversalPair, error) {
	var bad bool
	if !bytes.Equal(data, n.expectedData) {
		n.t.Errorf("got wrong bytes in Traverse: expected %#v; got %#v", n.expectedData, data)
		bad = true
	}
	if !c.Equals(n.expectedCid) {
		n.t.Errorf("got wrong cid: expected %v; got %v", n.expectedCid, c)
		bad = true
	}
	if bad {
		return []ipsl.CidTraversalPair{}, nil
	}

	return n.results, nil
}

type mockByteBlockGetter map[cid.Cid][]byte

func (g mockByteBlockGetter) GetBlock(_ context.Context, c cid.Cid) ([]byte, error) {
	b, ok := g[c]
	if !ok {
		panic(fmt.Sprintf("missing block requested %v", c))
	}
	return b, nil
}

func TestSyncDFS(t *testing.T) {
	ctx := context.Background()

	pref := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	}

	// root1 -> {leaf1, root2 -> {leaf2, leaf3}, leaf4}

	root1 := []byte("root1")
	root1Cid, err := pref.Sum(root1)
	if err != nil {
		t.Fatalf("hashing: %v", err)
	}
	leaf1 := []byte("leaf1")
	leaf1Cid, err := pref.Sum(leaf1)
	if err != nil {
		t.Fatalf("hashing: %v", err)
	}
	root2 := []byte("root2")
	root2Cid, err := pref.Sum(root2)
	if err != nil {
		t.Fatalf("hashing: %v", err)
	}
	leaf2 := []byte("leaf2")
	leaf2Cid, err := pref.Sum(leaf2)
	if err != nil {
		t.Fatalf("hashing: %v", err)
	}
	leaf3 := []byte("leaf3")
	leaf3Cid, err := pref.Sum(leaf3)
	if err != nil {
		t.Fatalf("hashing: %v", err)
	}
	leaf4 := []byte("leaf4")
	leaf4Cid, err := pref.Sum(leaf4)
	if err != nil {
		t.Fatalf("hashing: %v", err)
	}

	getter := mockByteBlockGetter{
		root1Cid: root1,
		leaf1Cid: leaf1,
		root2Cid: root2,
		leaf2Cid: leaf2,
		leaf3Cid: leaf3,
		leaf4Cid: leaf4,
	}

	traversal := mockTraversal{t, root1Cid, root1, []ipsl.CidTraversalPair{
		{Cid: leaf1Cid, Traversal: mockTraversal{t, leaf1Cid, leaf1, nil}},
		{Cid: root2Cid, Traversal: mockTraversal{t, root2Cid, root2, []ipsl.CidTraversalPair{
			{Cid: leaf2Cid, Traversal: mockTraversal{t, leaf2Cid, leaf2, nil}},
			{Cid: leaf3Cid, Traversal: mockTraversal{t, leaf3Cid, leaf3, nil}},
		}}},
		{Cid: leaf4Cid, Traversal: mockTraversal{t, leaf4Cid, leaf4, nil}},
	}}

	var result []cid.Cid
	err = SyncDFS(ctx, root1Cid, traversal, getter, 10, func(c cid.Cid, data []byte) error {
		if realBytes := getter[c]; !bytes.Equal(data, realBytes) {
			t.Errorf("got wrong bytes in callBack: expected %#v; got %#v", realBytes, data)
		}

		result = append(result, c)

		return nil
	})
	if err != nil {
		t.Fatalf("SyncDFS: %s", err)
	}

	expectedOrder := []cid.Cid{
		root1Cid,
		leaf1Cid,
		root2Cid,
		leaf2Cid,
		leaf3Cid,
		leaf4Cid,
	}
	if !slices.Equal(result, expectedOrder) {
		t.Errorf("bad traversal order: expected: %#v; got %#v", expectedOrder, result)
	}
}
