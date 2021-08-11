package merkledag_test

import (
	"bytes"
	"context"
	"testing"

	. "github.com/ipfs/go-merkledag"
	mdtest "github.com/ipfs/go-merkledag/test"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

var sampleCid cid.Cid

func init() {
	var err error
	// make a test CID -- doesn't matter just to add as a link
	sampleCid, err = cid.Cast([]byte{1, 85, 0, 5, 0, 1, 2, 3, 4})
	if err != nil {
		panic(err)
	}
}

func TestStableCID(t *testing.T) {
	nd := &ProtoNode{}
	nd.SetData([]byte("foobar"))
	nd.SetLinks([]*ipld.Link{
		{Name: "a", Cid: sampleCid},
		{Name: "b", Cid: sampleCid},
		{Name: "c", Cid: sampleCid},
	})
	expected, err := cid.Decode("QmciCHWD9Q47VPX6naY3XsPZGnqVqbedAniGCcaHjBaCri")
	if err != nil {
		t.Fatal(err)
	}
	if !nd.Cid().Equals(expected) {
		t.Fatalf("Got CID %s, expected CID %s", nd.Cid(), expected)
	}
}

func TestRemoveLink(t *testing.T) {
	nd := &ProtoNode{}
	nd.SetLinks([]*ipld.Link{
		{Name: "a", Cid: sampleCid},
		{Name: "b", Cid: sampleCid},
		{Name: "a", Cid: sampleCid},
		{Name: "a", Cid: sampleCid},
		{Name: "c", Cid: sampleCid},
		{Name: "a", Cid: sampleCid},
	})

	err := nd.RemoveNodeLink("a")
	if err != nil {
		t.Fatal(err)
	}

	if len(nd.Links()) != 2 {
		t.Fatal("number of links incorrect")
	}

	if nd.Links()[0].Name != "b" {
		t.Fatal("link order wrong")
	}

	if nd.Links()[1].Name != "c" {
		t.Fatal("link order wrong")
	}

	// should fail
	err = nd.RemoveNodeLink("a")
	if err != ErrLinkNotFound {
		t.Fatal("should have failed to remove link")
	}

	// ensure nothing else got touched
	if len(nd.Links()) != 2 {
		t.Fatal("number of links incorrect")
	}

	if nd.Links()[0].Name != "b" {
		t.Fatal("link order wrong")
	}

	if nd.Links()[1].Name != "c" {
		t.Fatal("link order wrong")
	}
}

func TestFindLink(t *testing.T) {
	ctx := context.Background()

	ds := mdtest.Mock()
	ndEmpty := new(ProtoNode)
	err := ds.Add(ctx, ndEmpty)
	if err != nil {
		t.Fatal(err)
	}

	kEmpty := ndEmpty.Cid()

	nd := &ProtoNode{}
	nd.SetLinks([]*ipld.Link{
		{Name: "a", Cid: kEmpty},
		{Name: "c", Cid: kEmpty},
		{Name: "b", Cid: kEmpty},
	})

	err = ds.Add(ctx, nd)
	if err != nil {
		t.Fatal(err)
	}

	lnk, err := nd.GetNodeLink("b")
	if err != nil {
		t.Fatal(err)
	}

	if lnk.Name != "b" {
		t.Fatal("got wrong link back")
	}

	_, err = nd.GetNodeLink("f")
	if err != ErrLinkNotFound {
		t.Fatal("shouldnt have found link")
	}

	_, err = nd.GetLinkedNode(context.Background(), ds, "b")
	if err != nil {
		t.Fatal(err)
	}

	outnd, err := nd.UpdateNodeLink("b", nd)
	if err != nil {
		t.Fatal(err)
	}

	olnk, err := outnd.GetNodeLink("b")
	if err != nil {
		t.Fatal(err)
	}

	if olnk.Cid.String() == kEmpty.String() {
		t.Fatal("new link should have different hash")
	}
}

func TestNodeCopy(t *testing.T) {
	nd := &ProtoNode{}
	nd.SetLinks([]*ipld.Link{
		{Name: "a", Cid: sampleCid},
		{Name: "c", Cid: sampleCid},
		{Name: "b", Cid: sampleCid},
	})

	nd.SetData([]byte("testing"))

	ond := nd.Copy().(*ProtoNode)
	ond.SetData(nil)

	if nd.Data() == nil {
		t.Fatal("should be different objects")
	}
}

func TestJsonRoundtrip(t *testing.T) {
	nd := new(ProtoNode)
	nd.SetLinks([]*ipld.Link{
		{Name: "a", Cid: sampleCid},
		{Name: "c", Cid: sampleCid},
		{Name: "b", Cid: sampleCid},
	})
	nd.SetData([]byte("testing"))

	jb, err := nd.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	nn := new(ProtoNode)
	err = nn.UnmarshalJSON(jb)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(nn.Data(), nd.Data()) {
		t.Fatal("data wasnt the same")
	}

	if !nn.Cid().Equals(nd.Cid()) {
		t.Fatal("objects differed after marshaling")
	}
}
