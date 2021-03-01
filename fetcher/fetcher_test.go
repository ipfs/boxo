package fetcher_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	testinstance "github.com/ipfs/go-bitswap/testinstance"
	tn "github.com/ipfs/go-bitswap/testnet"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/fluent"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/magiconair/properties/assert"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-fetcher"
)

var _ cidlink.MulticodecDecoder = dagcbor.Decoder

func TestFetchIPLDPrimeNode(t *testing.T) {
	block, node, _ := encodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 3, func(na fluent.MapAssembler) {
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
	fetcherConfig := fetcher.NewFetcherConfig(wantsGetter)
	fetch := fetcherConfig.NewSession(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	retrievedNode, err := fetch.Block(ctx, block.Cid())
	require.NoError(t, err)
	assert.Equal(t, node, retrievedNode)
}

func TestFetchIPLDGraph(t *testing.T) {
	block3, node3, link3 := encodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("three").AssignBool(true)
	}))
	block4, node4, link4 := encodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("four").AssignBool(true)
	}))
	block2, node2, link2 := encodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 2, func(na fluent.MapAssembler) {
		na.AssembleEntry("link3").AssignLink(link3)
		na.AssembleEntry("link4").AssignLink(link4)
	}))
	block1, node1, _ := encodeBlock(fluent.MustBuildMap(basicnode.Prototype__Map{}, 3, func(na fluent.MapAssembler) {
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
	fetcherConfig := fetcher.NewFetcherConfig(wantsGetter)
	fetch := fetcherConfig.NewSession(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	nodeCh, errCh := fetch.BlockAll(ctx, block1.Cid())
	require.NoError(t, err)

	order := 0

Loop:
	for {
		select {
		case res, ok := <-nodeCh:
			if !ok {
				break Loop
			}

			switch order {
			case 0:
				assert.Equal(t, node1, res.Node)
			case 4:
				assert.Equal(t, node2, res.Node)
			case 5:
				assert.Equal(t, node3, res.Node)
			case 7:
				assert.Equal(t, node4, res.Node)
			}
			order++
		case err := <-errCh:
			require.FailNow(t, err.Error())
		}
	}

	// expect 10 nodes altogether including sub nodes
	assert.Equal(t, 10, order)
}

func encodeBlock(n ipld.Node) (blocks.Block, ipld.Node, ipld.Link) {
	lb := cidlink.LinkBuilder{cid.Prefix{
		Version:  1,
		Codec:    cid.DagCBOR,
		MhType:   0x17,
		MhLength: 20,
	}}
	var b blocks.Block
	lnk, err := lb.Build(context.Background(), ipld.LinkContext{}, n,
		func(ipld.LinkContext) (io.Writer, ipld.StoreCommitter, error) {
			buf := bytes.Buffer{}
			return &buf, func(lnk ipld.Link) error {
				clnk, ok := lnk.(cidlink.Link)
				if !ok {
					return fmt.Errorf("incorrect link type %v", lnk)
				}
				var err error
				b, err = blocks.NewBlockWithCid(buf.Bytes(), clnk.Cid)
				return err
			}, nil
		},
	)
	if err != nil {
		panic(err)
	}
	return b, n, lnk
}
