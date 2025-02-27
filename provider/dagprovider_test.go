package provider

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	bsfetcher "github.com/ipfs/boxo/fetcher/impl/blockservice"
	"github.com/ipfs/boxo/ipld/merkledag"
	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-unixfsnode"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/stretchr/testify/require"
)

func makeDAGProvider(t *testing.T, ctx context.Context) (KeyChanFunc, []cid.Cid) {
	t.Helper()

	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bserv := blockservice.New(bs, offline.Exchange(bs))
	ipldFetcher := bsfetcher.NewFetcherConfig(bserv)
	ipldFetcher.PrototypeChooser = dagpb.AddSupportToChooser(bsfetcher.DefaultPrototypeChooser)
	unixFSFetcher := ipldFetcher.WithReifier(unixfsnode.Reify)
	dserv := merkledag.NewDAGService(bserv)
	daggen := mdutils.NewDAGGenerator()

	root, allCids, err := daggen.MakeDagNode(dserv.Add, 5, 2)
	require.NoError(t, err)
	t.Logf("Generated %d CIDs", len(allCids))

	keyChanF := NewDAGProvider(root, unixFSFetcher)
	return keyChanF, allCids
}

func TestNewDAGProvider(t *testing.T) {
	ctx := context.Background()
	keyChanF, allCids := makeDAGProvider(t, ctx)
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
	keyChanF, _ := makeDAGProvider(t, ctx)
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
