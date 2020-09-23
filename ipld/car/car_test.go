package car

import (
	"bytes"
	"context"
	"testing"

	cid "github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	dstest "github.com/ipfs/go-merkledag/test"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"
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
	if err := WriteCar(context.Background(), dserv, []cid.Cid{nd3.Cid()}, buf); err != nil {
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

func TestRoundtripSelective(t *testing.T) {
	sourceBserv := dstest.Bserv()
	sourceBs := sourceBserv.Blockstore()
	dserv := dag.NewDAGService(sourceBserv)
	a := dag.NewRawNode([]byte("aaaa"))
	b := dag.NewRawNode([]byte("bbbb"))
	c := dag.NewRawNode([]byte("cccc"))

	nd1 := &dag.ProtoNode{}
	nd1.AddNodeLink("cat", a)

	nd2 := &dag.ProtoNode{}
	nd2.AddNodeLink("first", nd1)
	nd2.AddNodeLink("dog", b)
	nd2.AddNodeLink("repeat", nd1)

	nd3 := &dag.ProtoNode{}
	nd3.AddNodeLink("second", nd2)
	nd3.AddNodeLink("bear", c)

	assertAddNodes(t, dserv, a, b, c, nd1, nd2, nd3)

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	// the graph assembled above looks as follows, in order:
	// nd3 -> [c, nd2 -> [nd1 -> a, b, nd1 -> a]]
	// this selector starts at n3, and traverses a link at index 1 (nd2, the second link, zero indexed)
	// it then recursively traverses all of its children
	// the only node skipped is 'c' -- link at index 0 immediately below nd3
	// the purpose is simply to show we are not writing the entire dag underneath
	// nd3
	selector := ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
		efsb.Insert("Links",
			ssb.ExploreIndex(1, ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))))
	}).Node()

	sc := NewSelectiveCar(context.Background(), sourceBs, []Dag{Dag{Root: nd3.Cid(), Selector: selector}})

	// write car in one step
	buf := new(bytes.Buffer)
	blockCount := 0
	err := sc.Write(buf, func(block Block) error {
		blockCount++
		return nil
	})
	require.Equal(t, blockCount, 5)
	require.NoError(t, err)

	// create a new builder for two-step write
	sc2 := NewSelectiveCar(context.Background(), sourceBs, []Dag{Dag{Root: nd3.Cid(), Selector: selector}})

	// write car in two steps
	scp, err := sc2.Prepare()
	require.NoError(t, err)
	buf2 := new(bytes.Buffer)
	err = scp.Dump(buf2)
	require.NoError(t, err)

	// verify preparation step correctly assesed length and blocks
	require.Equal(t, scp.Size(), uint64(buf.Len()))
	require.Equal(t, len(scp.Cids()), blockCount)

	// verify equal data written by both methods
	require.Equal(t, buf.Bytes(), buf2.Bytes())

	// readout car and verify contents
	bserv := dstest.Bserv()
	ch, err := LoadCar(bserv.Blockstore(), buf)
	require.NoError(t, err)
	require.Equal(t, len(ch.Roots), 1)

	require.True(t, ch.Roots[0].Equals(nd3.Cid()))

	bs := bserv.Blockstore()
	for _, nd := range []format.Node{a, b, nd1, nd2, nd3} {
		has, err := bs.Has(nd.Cid())
		require.NoError(t, err)
		require.True(t, has)
	}

	for _, nd := range []format.Node{c} {
		has, err := bs.Has(nd.Cid())
		require.NoError(t, err)
		require.False(t, has)
	}
}
