package car

import (
	"bytes"
	"context"
	"testing"

	cid "github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	dstest "github.com/ipfs/go-merkledag/test"
)

func assertAddNodes(t *testing.T, ds format.DAGService, nds ...format.Node) {
	for _, nd := range nds {
		if err := ds.Add(context.Background(), nd); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRoundtrip(t *testing.T) {
	dserv := dstest.Mock()
	a := dag.NewRawNode([]byte("aaaa"))
	b := dag.NewRawNode([]byte("bbbb"))
	c := dag.NewRawNode([]byte("cccc"))

	nd1 := &dag.ProtoNode{}
	nd1.AddNodeLink("cat", a)

	nd2 := &dag.ProtoNode{}
	nd2.AddNodeLink("first", nd1)
	nd2.AddNodeLink("dog", b)

	nd3 := &dag.ProtoNode{}
	nd3.AddNodeLink("second", nd2)
	nd3.AddNodeLink("bear", c)

	assertAddNodes(t, dserv, a, b, c, nd1, nd2, nd3)

	buf := new(bytes.Buffer)
	if err := WriteCar(context.Background(), dserv, []*cid.Cid{nd3.Cid()}, buf); err != nil {
		t.Fatal(err)
	}

	bserv := dstest.Bserv()
	ch, err := LoadCar(bserv.Blockstore(), buf)
	if err != nil {
		t.Fatal(err)
	}

	if len(ch.Roots) != 1 {
		t.Fatal("should have one root")
	}

	if !ch.Roots[0].Equals(nd3.Cid()) {
		t.Fatal("got wrong cid")
	}

	bs := bserv.Blockstore()
	for _, nd := range []format.Node{a, b, c, nd1, nd2, nd3} {
		has, err := bs.Has(nd.Cid())
		if err != nil {
			t.Fatal(err)
		}

		if !has {
			t.Fatal("should have cid in blockstore")
		}
	}
}
