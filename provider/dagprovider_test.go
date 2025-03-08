package provider

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/fetcher"
	bsfetcher "github.com/ipfs/boxo/fetcher/impl/blockservice"
	"github.com/ipfs/boxo/ipld/merkledag"
	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/stretchr/testify/require"
)

type dagProviderHelper struct {
	Datastore      datastore.Datastore
	Blockstore     blockstore.Blockstore
	BlockService   blockservice.BlockService
	FetcherFactory fetcher.Factory
	DAGService     format.DAGService
	KeyChanF       KeyChanFunc
	Cids           []cid.Cid
}

func makeDAGProvider(t *testing.T, ctx context.Context, fanout, depth uint) *dagProviderHelper {
	t.Helper()

	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bserv := blockservice.New(bs, offline.Exchange(bs))
	ipldFetcher := bsfetcher.NewFetcherConfig(bserv)
	ipldFetcher.SkipNotFound = true
	ipldFetcher.PrototypeChooser = dagpb.AddSupportToChooser(bsfetcher.DefaultPrototypeChooser)
	unixFSFetcher := ipldFetcher.WithReifier(unixfsnode.Reify)

	dserv := merkledag.NewDAGService(bserv)
	daggen := mdutils.NewDAGGenerator()

	root, allCids, err := daggen.MakeDagNode(dserv.Add, fanout, depth)
	require.NoError(t, err)
	t.Logf("Generated %d CIDs. Root: %s", len(allCids), root)

	keyChanF := NewDAGProvider(root, unixFSFetcher)
	return &dagProviderHelper{
		Datastore:      ds,
		Blockstore:     bs,
		BlockService:   bserv,
		FetcherFactory: unixFSFetcher,
		DAGService:     dserv,
		KeyChanF:       keyChanF,
		Cids:           allCids,
	}
}

func TestNewDAGProvider(t *testing.T) {
	ctx := context.Background()
	dph := makeDAGProvider(t, ctx, 5, 2)
	keyChanF := dph.KeyChanF
	allCids := dph.Cids

	cidMap := make(map[cid.Cid]struct{})
	cidCh, err := keyChanF(ctx)
	require.NoError(t, err)

	for c := range cidCh {
		cidMap[c] = struct{}{}
	}

	t.Logf("Collected %d CIDs", len(cidMap))

	for _, c := range allCids {
		if _, ok := cidMap[c]; !ok {
			t.Errorf("%s not traversed", c)
		}
	}
	require.Equal(t, len(allCids), len(cidMap), "number of traversed CIDs does not match CIDs in DAG")
}

func TestNewDAGProviderCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dph := makeDAGProvider(t, ctx, 5, 2)
	keyChanF := dph.KeyChanF
	cidMap := make(map[cid.Cid]struct{})
	cidCh, err := keyChanF(ctx)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for c := range cidCh {
			cidMap[c] = struct{}{}
			if len(cidMap) == 3 {
				time.Sleep(time.Second)
			}
		}

	}()

	time.Sleep(500 * time.Millisecond)
	cancel()
	<-done

	t.Logf("Collected %d CIDs", len(cidMap))
	require.Equal(t, 3, len(cidMap), "number of traversed CIDs when cancelling the context should be 3")
}

func TestNewDAGProviderMissingBlocks(t *testing.T) {
	ctx := context.Background()
	dph := makeDAGProvider(t, ctx, 1, 10)
	keyChanF := dph.KeyChanF
	allCids := dph.Cids

	// Remove a block from the blockstore.
	// since we have a single deep branch of 10 items, we will only be able
	// to visit 0, 1 and 2
	err := dph.Blockstore.DeleteBlock(ctx, allCids[3])
	require.NoError(t, err)

	cidMap := make(map[cid.Cid]struct{})
	cidCh, err := keyChanF(ctx)
	require.NoError(t, err)

	for c := range cidCh {
		cidMap[c] = struct{}{}
	}

	t.Logf("Collected %d CIDs", len(cidMap))

	for _, c := range allCids[0:3] {
		if _, ok := cidMap[c]; !ok {
			t.Errorf("%s should have been traversed", c)
		}
	}

	for _, c := range allCids[3:] {
		if _, ok := cidMap[c]; ok {
			t.Errorf("%s should not have been traversed", c)
		}
	}
}
