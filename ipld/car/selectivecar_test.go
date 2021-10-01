package car_test

import (
	"bytes"
	"context"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	dstest "github.com/ipfs/go-merkledag/test"
	car "github.com/ipld/go-car"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"
)

func TestRoundtripSelective(t *testing.T) {
	sourceBserv := dstest.Bserv()
	sourceBs := sourceBserv.Blockstore()
	dserv := merkledag.NewDAGService(sourceBserv)
	a := merkledag.NewRawNode([]byte("aaaa"))
	b := merkledag.NewRawNode([]byte("bbbb"))
	c := merkledag.NewRawNode([]byte("cccc"))

	nd1 := &merkledag.ProtoNode{}
	nd1.AddNodeLink("cat", a)

	nd2 := &merkledag.ProtoNode{}
	nd2.AddNodeLink("first", nd1)
	nd2.AddNodeLink("dog", b)
	nd2.AddNodeLink("repeat", nd1)

	nd3 := &merkledag.ProtoNode{}
	nd3.AddNodeLink("second", nd2)
	nd3.AddNodeLink("bear", c)

	assertAddNodes(t, dserv, a, b, c, nd1, nd2, nd3)

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	// the graph assembled above looks as follows, in order:
	// nd3 -> [c, nd2 -> [nd1 -> a, b, nd1 -> a]]
	// this selector starts at n3, and traverses a link at index 1 (nd2, the second link, zero indexed)
	// it then recursively traverses all of its children
	// the only node skipped is 'c' -- link at index 0 immediately below nd3
	// the purpose is simply to show we are not writing the entire merkledag underneath
	// nd3
	selector := ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
		efsb.Insert("Links",
			ssb.ExploreIndex(1, ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))))
	}).Node()

	sc := car.NewSelectiveCar(context.Background(), sourceBs, []car.Dag{{Root: nd3.Cid(), Selector: selector}})

	// write car in one step
	buf := new(bytes.Buffer)
	blockCount := 0
	var oneStepBlocks []car.Block
	err := sc.Write(buf, func(block car.Block) error {
		oneStepBlocks = append(oneStepBlocks, block)
		blockCount++
		return nil
	})
	require.Equal(t, blockCount, 5)
	require.NoError(t, err)

	// create a new builder for two-step write
	sc2 := car.NewSelectiveCar(context.Background(), sourceBs, []car.Dag{{Root: nd3.Cid(), Selector: selector}})

	// write car in two steps
	var twoStepBlocks []car.Block
	scp, err := sc2.Prepare(func(block car.Block) error {
		twoStepBlocks = append(twoStepBlocks, block)
		return nil
	})
	require.NoError(t, err)
	buf2 := new(bytes.Buffer)
	err = scp.Dump(buf2)
	require.NoError(t, err)

	// verify preparation step correctly assesed length and blocks
	require.Equal(t, scp.Size(), uint64(buf.Len()))
	require.Equal(t, len(scp.Cids()), blockCount)

	// verify equal data written by both methods
	require.Equal(t, buf.Bytes(), buf2.Bytes())

	// verify equal blocks were passed to user block hook funcs
	require.Equal(t, oneStepBlocks, twoStepBlocks)

	// readout car and verify contents
	bserv := dstest.Bserv()
	ch, err := car.LoadCar(bserv.Blockstore(), buf)
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

func TestNoLinkRepeatSelective(t *testing.T) {
	sourceBserv := dstest.Bserv()
	sourceBs := countingReadStore{bs: sourceBserv.Blockstore()}
	dserv := merkledag.NewDAGService(sourceBserv)
	a := merkledag.NewRawNode([]byte("aaaa"))
	b := merkledag.NewRawNode([]byte("bbbb"))
	c := merkledag.NewRawNode([]byte("cccc"))

	nd1 := &merkledag.ProtoNode{}
	nd1.AddNodeLink("cat", a)

	nd2 := &merkledag.ProtoNode{}
	nd2.AddNodeLink("first", nd1)
	nd2.AddNodeLink("dog", b)
	nd2.AddNodeLink("repeat", nd1)

	nd3 := &merkledag.ProtoNode{}
	nd3.AddNodeLink("second", nd2)
	nd3.AddNodeLink("bear", c)
	nd3.AddNodeLink("bearagain1", c)
	nd3.AddNodeLink("bearagain2", c)
	nd3.AddNodeLink("bearagain3", c)

	assertAddNodes(t, dserv, a, b, c, nd1, nd2, nd3)

	t.Run("TraverseLinksOnlyOnce off", func(t *testing.T) {
		sourceBs.count = 0
		sc := car.NewSelectiveCar(context.Background(),
			&sourceBs,
			[]car.Dag{{Root: nd3.Cid(), Selector: selectorparse.CommonSelector_ExploreAllRecursively}},
		)

		buf := new(bytes.Buffer)
		blockCount := 0
		err := sc.Write(buf, func(block car.Block) error {
			blockCount++
			return nil
		})
		require.Equal(t, blockCount, 6)
		require.Equal(t, sourceBs.count, 11) // with TraverseLinksOnlyOnce off, we expect repeat block visits because our DAG has repeat links
		require.NoError(t, err)
	})

	t.Run("TraverseLinksOnlyOnce on", func(t *testing.T) {
		sourceBs.count = 0

		sc := car.NewSelectiveCar(context.Background(),
			&sourceBs,
			[]car.Dag{{Root: nd3.Cid(), Selector: selectorparse.CommonSelector_ExploreAllRecursively}},
			car.TraverseLinksOnlyOnce(),
		)

		buf := new(bytes.Buffer)
		blockCount := 0
		err := sc.Write(buf, func(block car.Block) error {
			blockCount++
			return nil
		})
		require.Equal(t, blockCount, 6)
		require.Equal(t, sourceBs.count, 6) // only 6 blocks to load, no duplicate loading expected
		require.NoError(t, err)
	})
}

func TestLinkLimitSelective(t *testing.T) {
	sourceBserv := dstest.Bserv()
	sourceBs := sourceBserv.Blockstore()
	dserv := merkledag.NewDAGService(sourceBserv)
	a := merkledag.NewRawNode([]byte("aaaa"))
	b := merkledag.NewRawNode([]byte("bbbb"))
	c := merkledag.NewRawNode([]byte("cccc"))

	nd1 := &merkledag.ProtoNode{}
	nd1.AddNodeLink("cat", a)

	nd2 := &merkledag.ProtoNode{}
	nd2.AddNodeLink("first", nd1)
	nd2.AddNodeLink("dog", b)
	nd2.AddNodeLink("repeat", nd1)

	nd3 := &merkledag.ProtoNode{}
	nd3.AddNodeLink("second", nd2)
	nd3.AddNodeLink("bear", c)

	assertAddNodes(t, dserv, a, b, c, nd1, nd2, nd3)

	sc := car.NewSelectiveCar(context.Background(),
		sourceBs,
		[]car.Dag{{Root: nd3.Cid(), Selector: selectorparse.CommonSelector_ExploreAllRecursively}},
		car.MaxTraversalLinks(2))

	buf := new(bytes.Buffer)
	blockCount := 0
	err := sc.Write(buf, func(block car.Block) error {
		blockCount++
		return nil
	})
	require.Equal(t, blockCount, 3) // root + 2
	require.Error(t, err)
	require.Regexp(t, "^traversal budget exceeded: budget for links reached zero while on path .*", err)
}

type countingReadStore struct {
	bs    car.ReadStore
	count int
}

func (rs *countingReadStore) Get(c cid.Cid) (blocks.Block, error) {
	rs.count++
	return rs.bs.Get(c)
}
