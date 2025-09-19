package dspinner

import (
	"context"
	"fmt"
	"testing"
	"time"

	bs "github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	mdag "github.com/ipfs/boxo/ipld/merkledag"
	ipfspin "github.com/ipfs/boxo/pinning/pinner"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

func TestCheckIfPinnedWithType(t *testing.T) {
	const (
		withNames    = true
		withoutNames = false
	)

	ctx := context.Background()
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)

	p, err := New(ctx, dstore, dserv)
	require.NoError(t, err)

	// Create test DAG structure:
	// aNode (recursive pin) -> bNode (also direct pin)
	// cNode (not pinned)

	// Create child node b - will be both directly and indirectly pinned
	bNode := mdag.NodeWithData([]byte("b data"))
	err = dserv.Add(ctx, bNode)
	require.NoError(t, err)
	bk := bNode.Cid()

	// Create unrelated node c - won't be pinned
	cNode := mdag.NodeWithData([]byte("c data"))
	err = dserv.Add(ctx, cNode)
	require.NoError(t, err)
	ck := cNode.Cid()

	// Create parent node a that links to b
	aNode := mdag.NodeWithData([]byte("a data"))
	err = aNode.AddNodeLink("child", bNode)
	require.NoError(t, err)
	err = dserv.Add(ctx, aNode)
	require.NoError(t, err)
	ak := aNode.Cid()

	// Pin bNode directly with a name
	p.PinWithMode(ctx, bk, ipfspin.Direct, "direct-pin-b")
	err = p.Flush(ctx)
	require.NoError(t, err)

	// Pin aNode recursively with a name (this makes bNode also indirectly pinned)
	p.PinWithMode(ctx, ak, ipfspin.Recursive, "recursive-pin-a")
	err = p.Flush(ctx)
	require.NoError(t, err)

	t.Run("Direct mode with names", func(t *testing.T) {
		pinned, err := p.CheckIfPinnedWithType(ctx, ipfspin.Direct, withNames, bk)
		require.NoError(t, err)
		require.Len(t, pinned, 1)
		require.Equal(t, ipfspin.Direct, pinned[0].Mode)
		require.Equal(t, "direct-pin-b", pinned[0].Name)
	})

	t.Run("Direct mode without names", func(t *testing.T) {
		pinned, err := p.CheckIfPinnedWithType(ctx, ipfspin.Direct, withoutNames, bk)
		require.NoError(t, err)
		require.Len(t, pinned, 1)
		require.Equal(t, ipfspin.Direct, pinned[0].Mode)
		require.Empty(t, pinned[0].Name)
	})

	t.Run("Recursive mode with names", func(t *testing.T) {
		pinned, err := p.CheckIfPinnedWithType(ctx, ipfspin.Recursive, withNames, ak)
		require.NoError(t, err)
		require.Len(t, pinned, 1)
		require.Equal(t, ipfspin.Recursive, pinned[0].Mode)
		require.Equal(t, "recursive-pin-a", pinned[0].Name)
	})

	t.Run("Indirect mode - bk should be indirect via ak", func(t *testing.T) {
		pinned, err := p.CheckIfPinnedWithType(ctx, ipfspin.Indirect, withoutNames, bk)
		require.NoError(t, err)
		require.Len(t, pinned, 1)
		require.Equal(t, ipfspin.Indirect, pinned[0].Mode)
		require.Equal(t, ak, pinned[0].Via)
	})

	t.Run("Any mode - direct takes precedence", func(t *testing.T) {
		pinned, err := p.CheckIfPinnedWithType(ctx, ipfspin.Any, withNames, bk)
		require.NoError(t, err)
		require.Len(t, pinned, 1)
		// bk is pinned both directly and indirectly, but direct takes precedence
		require.Equal(t, ipfspin.Direct, pinned[0].Mode)
		require.Equal(t, "direct-pin-b", pinned[0].Name)
	})

	t.Run("Not pinned", func(t *testing.T) {
		pinned, err := p.CheckIfPinnedWithType(ctx, ipfspin.Direct, withoutNames, ck)
		require.NoError(t, err)
		require.Len(t, pinned, 1)
		require.Equal(t, ipfspin.NotPinned, pinned[0].Mode)
	})

	t.Run("Multiple CIDs", func(t *testing.T) {
		pinned, err := p.CheckIfPinnedWithType(ctx, ipfspin.Any, withNames, ak, bk, ck)
		require.NoError(t, err)
		require.Len(t, pinned, 3)

		// Check results for each CID
		results := make(map[cid.Cid]ipfspin.Pinned)
		for _, p := range pinned {
			results[p.Key] = p
		}

		require.Equal(t, ipfspin.Recursive, results[ak].Mode)
		require.Equal(t, "recursive-pin-a", results[ak].Name)

		require.Equal(t, ipfspin.Direct, results[bk].Mode)
		require.Equal(t, "direct-pin-b", results[bk].Name)

		require.Equal(t, ipfspin.NotPinned, results[ck].Mode)
	})

	t.Run("Internal mode returns NotPinned", func(t *testing.T) {
		pinned, err := p.CheckIfPinnedWithType(ctx, ipfspin.Internal, withoutNames, bk)
		require.NoError(t, err)
		require.Len(t, pinned, 1)
		require.Equal(t, ipfspin.NotPinned, pinned[0].Mode)
	})

	t.Run("Invalid mode returns error", func(t *testing.T) {
		_, err := p.CheckIfPinnedWithType(ctx, ipfspin.Mode(99), withoutNames, bk)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid Pin Mode")
	})

	t.Run("CheckIfPinned delegates to CheckIfPinnedWithType", func(t *testing.T) {
		// CheckIfPinned should behave like CheckIfPinnedWithType with Any mode and no names
		pinned1, err := p.CheckIfPinned(ctx, ak, bk, ck)
		require.NoError(t, err)

		pinned2, err := p.CheckIfPinnedWithType(ctx, ipfspin.Any, withoutNames, ak, bk, ck)
		require.NoError(t, err)

		require.Equal(t, pinned1, pinned2)
	})

	t.Run("Context cancellation during indirect check with many pins", func(t *testing.T) {
		// Create many recursive pins to ensure we can hit the cancellation
		nodes := make([]cid.Cid, 50)
		for i := 0; i < 50; i++ {
			node := mdag.NodeWithData([]byte(fmt.Sprintf("recursive node %d", i)))
			err = dserv.Add(ctx, node)
			require.NoError(t, err)
			nodes[i] = node.Cid()

			p.PinWithMode(ctx, nodes[i], ipfspin.Recursive, fmt.Sprintf("recursive-%d", i))
		}
		err = p.Flush(ctx)
		require.NoError(t, err)

		// Create a context that we can cancel
		cancelCtx, cancel := context.WithCancel(ctx)

		// Start checking in a goroutine
		done := make(chan struct{})
		var checkErr error
		go func() {
			defer close(done)
			// Try to check for an indirect pin with many recursive pins to traverse
			_, checkErr = p.CheckIfPinnedWithType(cancelCtx, ipfspin.Indirect, withoutNames, ck)
		}()

		// Cancel the context quickly
		time.Sleep(1 * time.Millisecond)
		cancel()

		// Wait for the check to complete
		<-done

		// Should get a context cancellation error (or nil if it completed too fast)
		// The important thing is that it doesn't hang
		if checkErr != nil {
			require.Equal(t, context.Canceled, checkErr)
		}
	})

	t.Run("Context cancellation in checkIndirectPins", func(t *testing.T) {
		// Create a context that we can cancel immediately
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		// Try to check for indirect pins with cancelled context
		_, err = p.CheckIfPinnedWithType(cancelCtx, ipfspin.Indirect, withoutNames, ck)

		// Should get a context cancellation error
		require.Error(t, err)
		require.Equal(t, context.Canceled, err)
	})
}
