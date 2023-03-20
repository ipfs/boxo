package helpers_test

import (
	"context"
	"testing"
	"time"

	delay "github.com/ipfs/go-ipfs-delay"
	testinstance "github.com/ipfs/go-libipfs/bitswap/testinstance"
	tn "github.com/ipfs/go-libipfs/bitswap/testnet"
	"github.com/ipfs/go-libipfs/blockservice"
	"github.com/ipfs/go-libipfs/fetcher/helpers"
	bsfetcher "github.com/ipfs/go-libipfs/fetcher/impl/blockservice"
	"github.com/ipfs/go-libipfs/fetcher/testutil"
	mockrouting "github.com/ipfs/go-libipfs/routing/mock"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var bg = context.Background()

func TestFetchGraphToBlocks(t *testing.T) {
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

	err := hasBlock.Exchange.NotifyNewBlocks(bg, block1)
	require.NoError(t, err)
	err = hasBlock.Exchange.NotifyNewBlocks(bg, block2)
	require.NoError(t, err)
	err = hasBlock.Exchange.NotifyNewBlocks(bg, block3)
	require.NoError(t, err)
	err = hasBlock.Exchange.NotifyNewBlocks(bg, block4)
	require.NoError(t, err)

	wantsBlock := peers[1]
	defer wantsBlock.Exchange.Close()

	wantsGetter := blockservice.New(wantsBlock.Blockstore(), wantsBlock.Exchange)
	fetcherConfig := bsfetcher.NewFetcherConfig(wantsGetter)
	session := fetcherConfig.NewSession(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	results := []helpers.BlockResult{}
	err = helpers.BlockAll(ctx, session, cidlink.Link{Cid: block1.Cid()}, helpers.OnBlocks(func(res helpers.BlockResult) error {
		results = append(results, res)
		return nil
	}))
	require.NoError(t, err)

	assertBlocksInOrder(t, results, 4, map[int]ipld.Node{0: node1, 1: node2, 2: node3, 3: node4})
}

func TestFetchGraphToUniqueBlocks(t *testing.T) {
	block3, node3, link3 := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("three").AssignBool(true)
	}))
	block2, node2, link2 := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 2, func(na fluent.MapAssembler) {
		na.AssembleEntry("link3").AssignLink(link3)
	}))
	block1, node1, _ := testutil.EncodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 3, func(na fluent.MapAssembler) {
		na.AssembleEntry("foo").AssignBool(true)
		na.AssembleEntry("bar").AssignBool(false)
		na.AssembleEntry("nested").CreateMap(2, func(na fluent.MapAssembler) {
			na.AssembleEntry("link2").AssignLink(link2)
			na.AssembleEntry("link3").AssignLink(link3)
			na.AssembleEntry("nonlink").AssignString("zoo")
		})
	}))

	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(0*time.Millisecond))
	ig := testinstance.NewTestInstanceGenerator(net, nil, nil)
	defer ig.Close()

	peers := ig.Instances(2)
	hasBlock := peers[0]
	defer hasBlock.Exchange.Close()

	err := hasBlock.Exchange.NotifyNewBlocks(bg, block1)
	require.NoError(t, err)
	err = hasBlock.Exchange.NotifyNewBlocks(bg, block2)
	require.NoError(t, err)
	err = hasBlock.Exchange.NotifyNewBlocks(bg, block3)
	require.NoError(t, err)

	wantsBlock := peers[1]
	defer wantsBlock.Exchange.Close()

	wantsGetter := blockservice.New(wantsBlock.Blockstore(), wantsBlock.Exchange)
	fetcherConfig := bsfetcher.NewFetcherConfig(wantsGetter)
	session := fetcherConfig.NewSession(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	results := []helpers.BlockResult{}
	err = helpers.BlockAll(ctx, session, cidlink.Link{Cid: block1.Cid()}, helpers.OnUniqueBlocks(func(res helpers.BlockResult) error {
		results = append(results, res)
		return nil
	}))
	require.NoError(t, err)

	assertBlocksInOrder(t, results, 3, map[int]ipld.Node{0: node1, 1: node2, 2: node3})
}

func assertBlocksInOrder(t *testing.T, results []helpers.BlockResult, nodeCount int, nodes map[int]ipld.Node) {
	for order, res := range results {
		expectedNode, ok := nodes[order]
		if ok {
			assert.Equal(t, expectedNode, res.Node)
		}
	}

	assert.Equal(t, nodeCount, len(results))
}
