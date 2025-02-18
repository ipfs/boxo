package provider

import (
	"context"
	"fmt"
	logger "log"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/fetcher"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"

	"github.com/ipfs/boxo/ipld/merkledag"
	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	"github.com/ipfs/go-cid"
	ipldp "github.com/ipld/go-ipld-prime"
	"github.com/stretchr/testify/require"
)

type mockFetcherFactory struct {
	blockService blockservice.BlockService
}

func (m *mockFetcherFactory) NewSession(ctx context.Context) fetcher.Fetcher {
	return &mockFetcher{bs: m.blockService}
}

type mockFetcher struct {
	bs blockservice.BlockService
}

func (m *mockFetcher) BlockMatchingOfType(ctx context.Context, root ipldp.Link, selector ipldp.Node, _ ipldp.NodePrototype, cb fetcher.FetchCallback) error {
	logger.Println("Starting BlockMatchingOfType")
	cidLnk, ok := root.(cidlink.Link)
	if !ok {
		return fmt.Errorf("expected CID link")
	}

	blk, err := m.bs.GetBlock(ctx, cidLnk.Cid)
	if err != nil {
		return err
	}

	// Process root
	err = cb(fetcher.FetchResult{
		Node:          basicnode.NewString(string(blk.RawData())),
		LastBlockLink: root,
	})
	if err != nil {
		return err
	}

	// Get and process children
	if protoNode, ok := blk.(*merkledag.ProtoNode); ok {
		for _, link := range protoNode.Links() {
			childBlk, err := m.bs.GetBlock(ctx, link.Cid)
			if err != nil {
				continue
			}
			err = cb(fetcher.FetchResult{
				Node:          basicnode.NewString(string(childBlk.RawData())),
				LastBlockLink: cidlink.Link{Cid: link.Cid},
			})
			if err != nil {
				return err
			}
		}
	}
	logger.Println("OKKKK")
	return nil
}

func (m *mockFetcher) BlockOfType(ctx context.Context, link ipldp.Link, nodePrototype ipldp.NodePrototype) (ipldp.Node, error) {
	cidLnk, ok := link.(cidlink.Link)
	if !ok {
		return nil, fmt.Errorf("expected CID link")
	}

	blk, err := m.bs.GetBlock(ctx, cidLnk.Cid)
	if err != nil {
		return nil, err
	}

	builder := nodePrototype.NewBuilder()
	err = builder.AssignBytes(blk.RawData())
	if err != nil {
		return nil, err
	}
	return builder.Build(), nil
	// return nodePrototype.NewBuilder().AssignBytes(blk.RawData()).Build()
}

func (m *mockFetcher) NodeMatching(ctx context.Context, root ipldp.Node, selector ipldp.Node, cb fetcher.FetchCallback) error {
	return cb(fetcher.FetchResult{
		Node: root,
	})
}

func (m *mockFetcher) PrototypeFromLink(link ipldp.Link) (ipldp.NodePrototype, error) {
	return basicnode.Prototype.Any, nil
}

func TestNewDAGProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create test DAG
	dagGen := mdutils.NewDAGGenerator()
	bsrv := mdutils.Bserv()

	// Generate a DAG with fanout=2, depth=3
	root, expectedCids, err := dagGen.MakeDagBlock(bsrv.AddBlock, 2, 3)
	require.NoError(t, err)

	// Create mock fetcher using our root blockservice
	fetcherFactory := &mockFetcherFactory{blockService: bsrv}

	// Creating DAGProvider with our root and fetcher
	provider := NewDAGProvider(root, fetcherFactory)
	cidChan, err := provider(ctx)
	require.NoError(t, err)

	// collect all the CIDs from the provider
	var collectedCids []cid.Cid
	for c := range cidChan {
		collectedCids = append(collectedCids, c)
	}

	// Verify - Got all the CIDs
	require.Equal(t, len(expectedCids), len(collectedCids), "number of collected CIDs should match the expected number")

	// Creating maps for easier comparison
	expectedMap := make(map[string]bool)
	for _, c := range expectedCids {
		expectedMap[c.String()] = true
	}

	collectedMap := make(map[string]bool)
	for _, c := range collectedCids {
		collectedMap[c.String()] = true
	}

	// Verify each expected CID was collected
	for _, expected := range expectedCids {
		require.True(t, collectedMap[expected.String()],
			"expected CID %s not found in collected CIDs", expected)
	}

	// Verify no extra CIDs were collected
	for _, collected := range collectedCids {
		require.True(t, expectedMap[collected.String()],
			"unexpected CID %s found in collected CIDs", collected)
	}

}
