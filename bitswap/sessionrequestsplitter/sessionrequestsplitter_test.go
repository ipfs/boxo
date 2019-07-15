package sessionrequestsplitter

import (
	"context"
	"testing"

	"github.com/ipfs/go-bitswap/testutil"
)

func quadEaseOut(t float64) float64 { return t * t }

func TestSplittingRequests(t *testing.T) {
	ctx := context.Background()
	optimizedPeers := testutil.GenerateOptimizedPeers(10, 5, quadEaseOut)
	keys := testutil.GenerateCids(6)

	srs := New(ctx)

	partialRequests := srs.SplitRequest(optimizedPeers, keys)
	if len(partialRequests) != 2 {
		t.Fatal("Did not generate right number of partial requests")
	}
	for _, partialRequest := range partialRequests {
		if len(partialRequest.Peers) != 5 && len(partialRequest.Keys) != 3 {
			t.Fatal("Did not split request into even partial requests")
		}
	}
}

func TestSplittingRequestsTooFewKeys(t *testing.T) {
	ctx := context.Background()
	optimizedPeers := testutil.GenerateOptimizedPeers(10, 5, quadEaseOut)
	keys := testutil.GenerateCids(1)

	srs := New(ctx)

	partialRequests := srs.SplitRequest(optimizedPeers, keys)
	if len(partialRequests) != 1 {
		t.Fatal("Should only generate as many requests as keys")
	}
	for _, partialRequest := range partialRequests {
		if len(partialRequest.Peers) != 5 && len(partialRequest.Keys) != 1 {
			t.Fatal("Should still split peers up between keys")
		}
	}
}

func TestSplittingRequestsTooFewPeers(t *testing.T) {
	ctx := context.Background()
	optimizedPeers := testutil.GenerateOptimizedPeers(1, 1, quadEaseOut)
	keys := testutil.GenerateCids(6)

	srs := New(ctx)

	partialRequests := srs.SplitRequest(optimizedPeers, keys)
	if len(partialRequests) != 1 {
		t.Fatal("Should only generate as many requests as peers")
	}
	for _, partialRequest := range partialRequests {
		if len(partialRequest.Peers) != 1 && len(partialRequest.Keys) != 6 {
			t.Fatal("Should not split keys if there are not enough peers")
		}
	}
}

func TestSplittingRequestsIncreasingSplitDueToDupes(t *testing.T) {
	ctx := context.Background()
	optimizedPeers := testutil.GenerateOptimizedPeers(maxSplit, maxSplit, quadEaseOut)
	keys := testutil.GenerateCids(maxSplit)

	srs := New(ctx)

	for i := 0; i < maxSplit+minReceivedToAdjustSplit; i++ {
		srs.RecordDuplicateBlock()
	}

	partialRequests := srs.SplitRequest(optimizedPeers, keys)
	if len(partialRequests) != maxSplit {
		t.Fatal("Did not adjust split up as duplicates came in")
	}
}

func TestSplittingRequestsDecreasingSplitDueToNoDupes(t *testing.T) {
	ctx := context.Background()
	optimizedPeers := testutil.GenerateOptimizedPeers(maxSplit, maxSplit, quadEaseOut)
	keys := testutil.GenerateCids(maxSplit)

	srs := New(ctx)

	for i := 0; i < 5+minReceivedToAdjustSplit; i++ {
		srs.RecordUniqueBlock()
	}

	partialRequests := srs.SplitRequest(optimizedPeers, keys)
	if len(partialRequests) != 1 {
		t.Fatal("Did not adjust split down as unique blocks came in")
	}
}
