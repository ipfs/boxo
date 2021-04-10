package bsfetcher_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"

	testinstance "github.com/ipfs/go-bitswap/testinstance"
	tn "github.com/ipfs/go-bitswap/testnet"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-fetcher"
	"github.com/ipfs/go-fetcher/helpers"
	bsfetcher "github.com/ipfs/go-fetcher/impl/blockservice"
	"github.com/ipfs/go-fetcher/testutil"
	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchIPLDPrimeNode(t *testing.T) {
	block, node, _ := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 3, func(na fluent.MapAssembler) {
		na.AssembleEntry("foo").AssignBool(true)
		na.AssembleEntry("bar").AssignBool(false)
		na.AssembleEntry("nested").CreateMap(2, func(na fluent.MapAssembler) {
			na.AssembleEntry("nonlink").AssignString("zoo")
		})
	}))

	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(0*time.Millisecond))
	ig := testinstance.NewTestInstanceGenerator(net, nil, nil)
	defer ig.Close()

	peers := ig.Instances(2)
	hasBlock := peers[0]
	defer hasBlock.Exchange.Close()

	err := hasBlock.Exchange.HasBlock(block)
	require.NoError(t, err)

	wantsBlock := peers[1]
	defer wantsBlock.Exchange.Close()

	wantsGetter := blockservice.New(wantsBlock.Blockstore(), wantsBlock.Exchange)
	fetcherConfig := bsfetcher.NewFetcherConfig(wantsGetter)
	session := fetcherConfig.NewSession(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	retrievedNode, err := helpers.Block(ctx, session, cidlink.Link{Cid: block.Cid()})
	require.NoError(t, err)
	assert.Equal(t, node, retrievedNode)
}

func TestFetchIPLDGraph(t *testing.T) {
	block3, node3, link3 := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("three").AssignBool(true)
	}))
	block4, node4, link4 := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("four").AssignBool(true)
	}))
	block2, node2, link2 := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 2, func(na fluent.MapAssembler) {
		na.AssembleEntry("link3").AssignLink(link3)
		na.AssembleEntry("link4").AssignLink(link4)
	}))
	block1, node1, _ := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 3, func(na fluent.MapAssembler) {
		na.AssembleEntry("foo").AssignBool(true)
		na.AssembleEntry("bar").AssignBool(false)
		na.AssembleEntry("nested").CreateMap(2, func(na fluent.MapAssembler) {
			na.AssembleEntry("link2").AssignLink(link2)
			na.AssembleEntry("nonlink").AssignString("zoo")
		})
	}))

	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(0*time.Millisecond))
	ig := testinstance.NewTestInstanceGenerator(net, nil, nil)
	defer ig.Close()

	peers := ig.Instances(2)
	hasBlock := peers[0]
	defer hasBlock.Exchange.Close()

	err := hasBlock.Exchange.HasBlock(block1)
	require.NoError(t, err)
	err = hasBlock.Exchange.HasBlock(block2)
	require.NoError(t, err)
	err = hasBlock.Exchange.HasBlock(block3)
	require.NoError(t, err)
	err = hasBlock.Exchange.HasBlock(block4)
	require.NoError(t, err)

	wantsBlock := peers[1]
	defer wantsBlock.Exchange.Close()

	wantsGetter := blockservice.New(wantsBlock.Blockstore(), wantsBlock.Exchange)
	fetcherConfig := bsfetcher.NewFetcherConfig(wantsGetter)
	session := fetcherConfig.NewSession(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	results := []fetcher.FetchResult{}
	err = helpers.BlockAll(ctx, session, cidlink.Link{Cid: block1.Cid()}, func(res fetcher.FetchResult) error {
		results = append(results, res)
		return nil
	})
	require.NoError(t, err)

	assertNodesInOrder(t, results, 10, map[int]ipld.Node{0: node1, 4: node2, 5: node3, 7: node4})
}

func TestFetchIPLDPath(t *testing.T) {
	block5, node5, link5 := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("five").AssignBool(true)
	}))
	block3, _, link3 := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("three").AssignLink(link5)
	}))
	block4, _, link4 := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("four").AssignBool(true)
	}))
	block2, _, link2 := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 2, func(na fluent.MapAssembler) {
		na.AssembleEntry("link3").AssignLink(link3)
		na.AssembleEntry("link4").AssignLink(link4)
	}))
	block1, _, _ := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 3, func(na fluent.MapAssembler) {
		na.AssembleEntry("foo").AssignBool(true)
		na.AssembleEntry("bar").AssignBool(false)
		na.AssembleEntry("nested").CreateMap(2, func(na fluent.MapAssembler) {
			na.AssembleEntry("link2").AssignLink(link2)
			na.AssembleEntry("nonlink").AssignString("zoo")
		})
	}))

	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(0*time.Millisecond))
	ig := testinstance.NewTestInstanceGenerator(net, nil, nil)
	defer ig.Close()

	peers := ig.Instances(2)
	hasBlock := peers[0]
	defer hasBlock.Exchange.Close()

	for _, blk := range []blocks.Block{block1, block2, block3, block4, block5} {
		err := hasBlock.Exchange.HasBlock(blk)
		require.NoError(t, err)
	}

	wantsBlock := peers[1]
	defer wantsBlock.Exchange.Close()

	wantsGetter := blockservice.New(wantsBlock.Blockstore(), wantsBlock.Exchange)
	fetcherConfig := bsfetcher.NewFetcherConfig(wantsGetter)
	session := fetcherConfig.NewSession(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	path := strings.Split("nested/link2/link3/three", "/")
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	spec := ssb.Matcher()
	explorePath := func(p string, s builder.SelectorSpec) builder.SelectorSpec {
		return ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) { efsb.Insert(p, s) })
	}
	for i := len(path) - 1; i >= 0; i-- {
		spec = explorePath(path[i], spec)
	}
	sel := spec.Node()

	results := []fetcher.FetchResult{}
	err := helpers.BlockMatching(ctx, session, cidlink.Link{Cid: block1.Cid()}, sel, func(res fetcher.FetchResult) error {
		results = append(results, res)
		return nil
	})
	require.NoError(t, err)

	assertNodesInOrder(t, results, 1, map[int]ipld.Node{0: node5})
}

func TestHelpers(t *testing.T) {
	block3, node3, link3 := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("three").AssignBool(true)
	}))
	block4, node4, link4 := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("four").AssignBool(true)
	}))
	block2, node2, link2 := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 2, func(na fluent.MapAssembler) {
		na.AssembleEntry("link3").AssignLink(link3)
		na.AssembleEntry("link4").AssignLink(link4)
	}))
	block1, node1, _ := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 3, func(na fluent.MapAssembler) {
		na.AssembleEntry("foo").AssignBool(true)
		na.AssembleEntry("bar").AssignBool(false)
		na.AssembleEntry("nested").CreateMap(2, func(na fluent.MapAssembler) {
			na.AssembleEntry("link2").AssignLink(link2)
			na.AssembleEntry("nonlink").AssignString("zoo")
		})
	}))

	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(0*time.Millisecond))
	ig := testinstance.NewTestInstanceGenerator(net, nil, nil)
	defer ig.Close()

	peers := ig.Instances(2)
	hasBlock := peers[0]
	defer hasBlock.Exchange.Close()

	err := hasBlock.Exchange.HasBlock(block1)
	require.NoError(t, err)
	err = hasBlock.Exchange.HasBlock(block2)
	require.NoError(t, err)
	err = hasBlock.Exchange.HasBlock(block3)
	require.NoError(t, err)
	err = hasBlock.Exchange.HasBlock(block4)
	require.NoError(t, err)

	wantsBlock := peers[1]
	defer wantsBlock.Exchange.Close()

	wantsGetter := blockservice.New(wantsBlock.Blockstore(), wantsBlock.Exchange)

	t.Run("Block retrieves node", func(t *testing.T) {
		fetcherConfig := bsfetcher.NewFetcherConfig(wantsGetter)
		session := fetcherConfig.NewSession(context.Background())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		node, err := helpers.Block(ctx, session, cidlink.Link{Cid: block1.Cid()})
		require.NoError(t, err)

		assert.Equal(t, node, node1)
	})

	t.Run("BlockMatching retrieves nodes matching selector", func(t *testing.T) {
		// limit recursion depth to 2 nodes and expect to get only 2 blocks (4 nodes)
		ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype__Any{})
		sel := ssb.ExploreRecursive(selector.RecursionLimitDepth(2), ssb.ExploreUnion(
			ssb.Matcher(),
			ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
		)).Node()

		fetcherConfig := bsfetcher.NewFetcherConfig(wantsGetter)
		session := fetcherConfig.NewSession(context.Background())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		results := []fetcher.FetchResult{}
		err = helpers.BlockMatching(ctx, session, cidlink.Link{Cid: block1.Cid()}, sel, func(res fetcher.FetchResult) error {
			results = append(results, res)
			return nil
		})
		require.NoError(t, err)

		assertNodesInOrder(t, results, 4, map[int]ipld.Node{0: node1, 4: node2})
	})

	t.Run("BlockAllOfType retrieves all nodes with a schema", func(t *testing.T) {
		// limit recursion depth to 2 nodes and expect to get only 2 blocks (4 nodes)
		fetcherConfig := bsfetcher.NewFetcherConfig(wantsGetter)
		session := fetcherConfig.NewSession(context.Background())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		results := []fetcher.FetchResult{}
		err = helpers.BlockAllOfType(ctx, session, cidlink.Link{Cid: block1.Cid()}, basicnode.Prototype__Any{}, func(res fetcher.FetchResult) error {
			results = append(results, res)
			return nil
		})
		require.NoError(t, err)

		assertNodesInOrder(t, results, 10, map[int]ipld.Node{0: node1, 4: node2, 5: node3, 7: node4})
	})
}

func assertNodesInOrder(t *testing.T, results []fetcher.FetchResult, nodeCount int, nodes map[int]ipld.Node) {
	for order, res := range results {
		expectedNode, ok := nodes[order]
		if ok {
			assert.Equal(t, expectedNode, res.Node)
		}
	}

	assert.Equal(t, nodeCount, len(results))
}

type selfLoader struct {
	ipld.Node
	ctx context.Context
	ls  *ipld.LinkSystem
}

func (sl *selfLoader) LookupByString(key string) (ipld.Node, error) {
	nd, err := sl.Node.LookupByString(key)
	if err != nil {
		return nd, err
	}
	if nd.Kind() == ipld.Kind_Link {
		lnk, _ := nd.AsLink()
		nd, err = sl.ls.Load(ipld.LinkContext{Ctx: sl.ctx}, lnk, basicnode.Prototype.Any)
	}
	return nd, err
}

func TestNodeReification(t *testing.T) {
	// demonstrates how to use the augment chooser to build an ADL that self loads its own nodes
	block3, node3, link3 := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("three").AssignBool(true)
	}))
	block4, node4, link4 := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("four").AssignBool(true)
	}))
	block2, _, _ := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 2, func(na fluent.MapAssembler) {
		na.AssembleEntry("link3").AssignLink(link3)
		na.AssembleEntry("link4").AssignLink(link4)
	}))

	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(0*time.Millisecond))
	ig := testinstance.NewTestInstanceGenerator(net, nil, nil)
	defer ig.Close()

	peers := ig.Instances(2)
	hasBlock := peers[0]
	defer hasBlock.Exchange.Close()

	err := hasBlock.Exchange.HasBlock(block2)
	require.NoError(t, err)
	err = hasBlock.Exchange.HasBlock(block3)
	require.NoError(t, err)
	err = hasBlock.Exchange.HasBlock(block4)
	require.NoError(t, err)

	wantsBlock := peers[1]
	defer wantsBlock.Exchange.Close()

	wantsGetter := blockservice.New(wantsBlock.Blockstore(), wantsBlock.Exchange)
	fetcherConfig := bsfetcher.NewFetcherConfig(wantsGetter)
	nodeReifier := func(lnkCtx ipld.LinkContext, nd ipld.Node, ls *ipld.LinkSystem) (ipld.Node, error) {
		return &selfLoader{Node: nd, ctx: lnkCtx.Ctx, ls: ls}, nil
	}
	fetcherConfig.NodeReifier = nodeReifier
	session := fetcherConfig.NewSession(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	retrievedNode, err := helpers.Block(ctx, session, cidlink.Link{Cid: block2.Cid()})
	require.NoError(t, err)

	// instead of getting links back, we automatically load the nodes

	retrievedNode3, err := retrievedNode.LookupByString("link3")
	require.NoError(t, err)
	underlying3 := retrievedNode3.(*selfLoader).Node
	assert.Equal(t, node3, underlying3)

	retrievedNode4, err := retrievedNode.LookupByString("link4")
	require.NoError(t, err)
	underlying4 := retrievedNode4.(*selfLoader).Node
	assert.Equal(t, node4, underlying4)

}
