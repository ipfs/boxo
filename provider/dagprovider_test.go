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

	visited := make(map[string]bool)
	processed := 0

	var traverse func(ipldp.Link) error
	traverse = func(link ipldp.Link) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		cidLnk, ok := link.(cidlink.Link)
		if !ok {
			return fmt.Errorf("expected CID link")
		}

		cidStr := cidLnk.Cid.String()
		if visited[cidStr] {
			logger.Printf("Already visited CID: %s", cidStr)
			return nil
		}

		// Add select for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			blk, err := m.bs.GetBlock(ctx, cidLnk.Cid)
			if err != nil {
				logger.Printf("Error getting block %s: %s", cidStr, err)
				return err
			}

			visited[cidStr] = true
			processed++
			logger.Printf("Processing CID %s (%d processed)", cidStr, processed)

			// Add select for callback
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				err = cb(fetcher.FetchResult{
					Node:          basicnode.NewString(string(blk.RawData())),
					LastBlockLink: link,
				})
				if err != nil {
					logger.Printf("Callback error for CID %s: %s", cidStr, err)
					return err
				}
			}

			if protoNode, ok := blk.(*merkledag.ProtoNode); ok {
				links := protoNode.Links()
				logger.Printf("Found %d links in CID %s", len(links), cidStr)

				for i, link := range links {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						logger.Printf("Traversing child %d/%d of CID %s: %s",
							i+1, len(links), cidStr, link.Cid)

						if err := traverse(cidlink.Link{Cid: link.Cid}); err != nil {
							return err
						}
					}
				}
			}
		}

		return nil
	}

	err := traverse(root)
	if err != nil {
		logger.Printf("Traversal error: %s", err)
		return err
	}

	logger.Printf("Completed DAG traversal, processed %d CIDs", processed)
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
	t.Logf("Created DAG with root %s and %d expected CIDs", root, len(expectedCids))
	for i, c := range expectedCids {
		t.Logf("Expected CID %d: %s", i+1, c)
	}

	// Create mock fetcher using our root blockservice
	fetcherFactory := &mockFetcherFactory{blockService: bsrv}

	// Creating DAGProvider with our root and fetcher
	provider := NewDAGProvider(root, fetcherFactory)
	cidChan, err := provider(ctx)
	require.NoError(t, err)

	// collect all the CIDs from the provider
	var collectedCids []cid.Cid
	for c := range cidChan {
		t.Logf("Collected CID: %s", c)
		collectedCids = append(collectedCids, c)
	}

	t.Logf("Collected %d CIDs", len(collectedCids))

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
