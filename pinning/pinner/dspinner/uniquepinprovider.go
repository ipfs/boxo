package dspinner

import (
	"context"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/dag/walker"
	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
	"github.com/ipfs/boxo/provider"
	"github.com/ipfs/go-cid"
)

// NewUniquePinnedProvider returns a [provider.KeyChanFunc] that emits
// all blocks reachable from pinned roots, with bloom filter cross-pin
// deduplication via the shared [walker.VisitedTracker].
//
// Processing order: recursive pin DAGs first (via [walker.WalkDAG]),
// then direct pins. This order ensures that by the time direct pins
// are processed, all recursive DAGs have been walked and their CIDs
// are in the tracker.
//
// The existing [NewPinnedProvider] is unchanged. This function is used
// only when the +unique strategy modifier is active.
func NewUniquePinnedProvider(
	pinning ipfspinner.Pinner,
	bs blockstore.Blockstore,
	tracker walker.VisitedTracker,
) provider.KeyChanFunc {
	return func(ctx context.Context) (<-chan cid.Cid, error) {
		outCh := make(chan cid.Cid)

		go func() {
			defer close(outCh)

			emit := func(c cid.Cid) bool {
				select {
				case outCh <- c:
					return true
				case <-ctx.Done():
					return false
				}
			}

			fetch := walker.LinksFetcherFromBlockstore(bs)

			// 1. Walk recursive pin DAGs (bulk of dedup benefit)
			for sc := range pinning.RecursiveKeys(ctx, false) {
				if sc.Err != nil {
					log.Errorf("unique provide recursive pins: %s", sc.Err)
					return
				}
				if err := walker.WalkDAG(ctx, sc.Pin.Key, fetch, emit, walker.WithVisitedTracker(tracker)); err != nil {
					return // context cancelled
				}
			}

			// 2. Direct pins (emit if not already visited)
			for sc := range pinning.DirectKeys(ctx, false) {
				if sc.Err != nil {
					log.Errorf("unique provide direct pins: %s", sc.Err)
					return
				}
				if tracker.Visit(sc.Pin.Key) {
					if !emit(sc.Pin.Key) {
						return
					}
				}
			}
		}()

		return outCh, nil
	}
}

// NewPinnedEntityRootsProvider returns a [provider.KeyChanFunc] that
// emits entity roots (files, directories, HAMT shards) reachable from
// pinned roots, skipping internal file chunks. Uses
// [walker.WalkEntityRoots] with the shared [walker.VisitedTracker]
// for cross-pin deduplication.
//
// Same processing order as [NewUniquePinnedProvider]: recursive pins
// first, direct pins second.
func NewPinnedEntityRootsProvider(
	pinning ipfspinner.Pinner,
	bs blockstore.Blockstore,
	tracker walker.VisitedTracker,
) provider.KeyChanFunc {
	return func(ctx context.Context) (<-chan cid.Cid, error) {
		outCh := make(chan cid.Cid)

		go func() {
			defer close(outCh)

			emit := func(c cid.Cid) bool {
				select {
				case outCh <- c:
					return true
				case <-ctx.Done():
					return false
				}
			}

			fetch := walker.NodeFetcherFromBlockstore(bs)

			// 1. Walk recursive pin DAGs for entity roots
			for sc := range pinning.RecursiveKeys(ctx, false) {
				if sc.Err != nil {
					log.Errorf("entity provide recursive pins: %s", sc.Err)
					return
				}
				if err := walker.WalkEntityRoots(ctx, sc.Pin.Key, fetch, emit, walker.WithVisitedTracker(tracker)); err != nil {
					return
				}
			}

			// 2. Direct pins (always entity roots by definition)
			for sc := range pinning.DirectKeys(ctx, false) {
				if sc.Err != nil {
					log.Errorf("entity provide direct pins: %s", sc.Err)
					return
				}
				if tracker.Visit(sc.Pin.Key) {
					if !emit(sc.Pin.Key) {
						return
					}
				}
			}
		}()

		return outCh, nil
	}
}
