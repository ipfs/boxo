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
	ipinner "github.com/ipfs/boxo/pinning/pinner"
	"github.com/ipfs/boxo/pinning/pinner/dspinner"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

// TestBufferedPinProvider checks that we can modify a pinset while reading
// from the provider, as all elements of the pinset have been placed in
// memory.
func TestBufferedPinProvider(t *testing.T) {
	ctx := context.Background()

	// Setup
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bserv := blockservice.New(bs, offline.Exchange(bs))
	fetcher := bsfetcher.NewFetcherConfig(bserv)
	dserv := merkledag.NewDAGService(bserv)
	pinner, err := dspinner.New(ctx, ds, dserv)
	require.NoError(t, err)
	daggen := mdutils.NewDAGGenerator()
	root, _, err := daggen.MakeDagNode(dserv.Add, 1, 64)
	require.NoError(t, err)
	root2, _, err := daggen.MakeDagNode(dserv.Add, 1, 64)
	require.NoError(t, err)

	// test with 0 pins to ensure things work.
	zeroProv := NewPinnedProvider(false, pinner, fetcher)
	zeroKeyChanF := NewBufferedProvider(zeroProv)
	zeroPins, err := zeroKeyChanF(ctx)
	require.NoError(t, err)
	for range zeroPins {
		t.Error("There should not be any pins")
	}

	// Pin the first DAG.
	err = pinner.PinWithMode(ctx, root, ipinner.Recursive, "test")
	require.NoError(t, err)

	// Then open the keyChanF to read the pins. This should trigger the
	// pin query, but we don't read from it, so in normal condiditions
	// it would block.
	pinProv := NewPinnedProvider(false, pinner, fetcher)
	keyChanF := NewBufferedProvider(pinProv)
	root1pins, err := keyChanF(ctx)
	require.NoError(t, err)

	// Give time to buffer all the results as this is happening in the
	// background.
	time.Sleep(200 * time.Millisecond)

	// If the previous query was blocking the pinset under a read-lock,
	// we would not be able to write a second pin:
	err = pinner.PinWithMode(ctx, root2, ipinner.Recursive, "test")
	require.NoError(t, err)

	// Now we trigger a second query.
	pinProv2 := NewPinnedProvider(false, pinner, fetcher)
	keyChanF2 := NewBufferedProvider(pinProv2)
	root2pins, err := keyChanF2(ctx)
	require.NoError(t, err)

	// And finally proceed to read pins. The second keyChan should contain
	// both root and root2 pins, while the first keyChan contains only the
	// elements from the first pin because they were all cached before the
	// second pin happened.
	root1count := 0
	root2count := 0
	for range root2pins {
		root2count++
	}
	for range root1pins {
		root1count++
	}
	require.Equal(t, 64, root1count, "first pin should have provided 2048 cids")
	require.Equal(t, 64+64, root2count, "second pin should have provided 4096 cids")
}
