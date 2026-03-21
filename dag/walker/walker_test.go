package walker_test

import (
	"context"
	"fmt"
	"testing"

	blockstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/dag/walker"
	"github.com/ipfs/boxo/ipld/merkledag"
	mdtest "github.com/ipfs/boxo/ipld/merkledag/test"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/ipld/unixfs/hamt"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBlockstore() blockstore.Blockstore {
	return blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
}

func buildDAG(t *testing.T, bs blockstore.Blockstore, fanout, depth uint) (cid.Cid, []cid.Cid) {
	t.Helper()
	dserv := merkledag.NewDAGService(mdtest.Bserv())
	gen := mdtest.NewDAGGenerator()
	root, allCids, err := gen.MakeDagNode(dserv.Add, fanout, depth)
	require.NoError(t, err)
	for _, c := range allCids {
		nd, err := dserv.Get(t.Context(), c)
		require.NoError(t, err)
		require.NoError(t, bs.Put(t.Context(), nd))
	}
	return root, allCids
}

func putRawBlock(t *testing.T, bs blockstore.Blockstore, data []byte) cid.Cid {
	t.Helper()
	hash, _ := mh.Sum(data, mh.SHA2_256, -1)
	c := cid.NewCidV1(cid.Raw, hash)
	blk, err := blocks.NewBlockWithCid(data, c)
	require.NoError(t, err)
	require.NoError(t, bs.Put(t.Context(), blk))
	return c
}

func collectWalk(t *testing.T, bs blockstore.Blockstore, root cid.Cid, opts ...walker.Option) []cid.Cid {
	t.Helper()
	var visited []cid.Cid
	fetch := walker.LinksFetcherFromBlockstore(bs)
	err := walker.WalkDAG(t.Context(), root, fetch, func(c cid.Cid) bool {
		visited = append(visited, c)
		return true
	}, opts...)
	require.NoError(t, err)
	return visited
}

func TestWalkDAG_Traversal(t *testing.T) {
	// Verify the walker visits every node in a DAG exactly once
	// and in DFS pre-order (root first).
	t.Run("visits all nodes in a multi-level DAG", func(t *testing.T) {
		bs := newTestBlockstore()
		root, allCids := buildDAG(t, bs, 3, 2)

		visited := collectWalk(t, bs, root)
		assert.Len(t, visited, len(allCids),
			"should visit every node in the DAG")

		visitedSet := make(map[cid.Cid]struct{})
		for _, c := range visited {
			visitedSet[c] = struct{}{}
		}
		for _, c := range allCids {
			assert.Contains(t, visitedSet, c,
				"every CID in the DAG should be visited")
		}
	})

	// The root CID must always be the first emitted CID, which is
	// critical for ExecuteFastProvideRoot (root must be announced
	// before any other block).
	t.Run("root is always the first CID emitted", func(t *testing.T) {
		bs := newTestBlockstore()
		root, _ := buildDAG(t, bs, 3, 2)

		visited := collectWalk(t, bs, root)
		require.NotEmpty(t, visited)
		assert.Equal(t, root, visited[0])
	})

	// A single raw block with no links should be walked as a
	// single-node DAG (common case: small files stored as raw leaves).
	t.Run("single leaf node with no children", func(t *testing.T) {
		bs := newTestBlockstore()
		leaf := putRawBlock(t, bs, []byte("leaf data"))

		visited := collectWalk(t, bs, leaf)
		assert.Len(t, visited, 1)
		assert.Equal(t, leaf, visited[0])
	})

	// When no VisitedTracker is provided, the walker should still
	// visit every node (no dedup, no crash).
	t.Run("works without any tracker", func(t *testing.T) {
		bs := newTestBlockstore()
		root, allCids := buildDAG(t, bs, 2, 2)

		visited := collectWalk(t, bs, root) // no WithVisitedTracker
		assert.Len(t, visited, len(allCids))
	})

	// DAG diamond: root -> {A, B}, A -> {C}, B -> {C}.
	// With a tracker, C must be visited exactly once even though two
	// paths lead to it. Without a tracker, C would be visited twice.
	t.Run("DAG diamond: shared child visited once with tracker", func(t *testing.T) {
		bs := newTestBlockstore()
		dserv := merkledag.NewDAGService(mdtest.Bserv())

		leafC := merkledag.NodeWithData([]byte("shared-leaf-C"))
		require.NoError(t, dserv.Add(t.Context(), leafC))

		nodeA := merkledag.NodeWithData([]byte("node-A"))
		nodeA.AddNodeLink("c", leafC)
		require.NoError(t, dserv.Add(t.Context(), nodeA))

		nodeB := merkledag.NodeWithData([]byte("node-B"))
		nodeB.AddNodeLink("c", leafC)
		require.NoError(t, dserv.Add(t.Context(), nodeB))

		root := merkledag.NodeWithData([]byte("root"))
		root.AddNodeLink("a", nodeA)
		root.AddNodeLink("b", nodeB)
		require.NoError(t, dserv.Add(t.Context(), root))

		for _, nd := range []merkledag.ProtoNode{*root, *nodeA, *nodeB, *leafC} {
			require.NoError(t, bs.Put(t.Context(), &nd))
		}

		tracker := walker.NewMapTracker()
		visited := collectWalk(t, bs, root.Cid(), walker.WithVisitedTracker(tracker))

		cCount := 0
		for _, v := range visited {
			if v == leafC.Cid() {
				cCount++
			}
		}
		assert.Equal(t, 1, cCount,
			"shared child C must be visited exactly once")
		assert.Len(t, visited, 4, // root, A, B, C
			"diamond DAG has 4 unique nodes")
	})

	// HAMT sharded directories are multi-level dag-pb structures where
	// internal shard buckets are separate blocks. WalkDAG must visit
	// every internal shard node and every leaf entry node. This is
	// critical for provide: all HAMT layers must be announced so peers
	// can enumerate the directory.
	t.Run("HAMT sharded directory: all internal shard nodes visited", func(t *testing.T) {
		bs := newTestBlockstore()
		dserv := merkledag.NewDAGService(mdtest.Bserv())

		// build a HAMT with 500 entries to force multiple shard levels.
		// half are empty dirs (all share the same CID -- tests dedup
		// across repeated leaves), half are unique files (distinct CIDs).
		const nEntries = 500
		shard, err := hamt.NewShard(dserv, 256)
		require.NoError(t, err)
		leafCids := make(map[cid.Cid]struct{})
		emptyDir := ft.EmptyDirNode()
		require.NoError(t, dserv.Add(t.Context(), emptyDir))
		for i := range nEntries {
			name := fmt.Sprintf("entry-%04d", i)
			if i%2 == 0 {
				// empty dir (shared CID across all even entries)
				require.NoError(t, shard.Set(t.Context(), name, emptyDir))
				leafCids[emptyDir.Cid()] = struct{}{}
			} else {
				// unique file
				leaf := merkledag.NodeWithData([]byte(fmt.Sprintf("file-%04d", i)))
				require.NoError(t, dserv.Add(t.Context(), leaf))
				require.NoError(t, shard.Set(t.Context(), name, leaf))
				leafCids[leaf.Cid()] = struct{}{}
			}
		}

		// serialize the HAMT (writes all shard nodes to dserv)
		rootNd, err := shard.Node()
		require.NoError(t, err)
		rootCid := rootNd.Cid()

		// collect all CIDs reachable from root via dserv (ground truth)
		allCids := make(map[cid.Cid]struct{})
		var enumerate func(c cid.Cid)
		enumerate = func(c cid.Cid) {
			if _, ok := allCids[c]; ok {
				return
			}
			allCids[c] = struct{}{}
			nd, err := dserv.Get(t.Context(), c)
			if err != nil {
				return
			}
			for _, lnk := range nd.Links() {
				enumerate(lnk.Cid)
			}
		}
		enumerate(rootCid)

		// copy all blocks to test blockstore
		for c := range allCids {
			nd, err := dserv.Get(t.Context(), c)
			require.NoError(t, err)
			require.NoError(t, bs.Put(t.Context(), nd))
		}

		// walk with tracker to dedup (HAMT leaf nodes are unique but
		// the walker without tracker would revisit them via each shard)
		tracker := walker.NewMapTracker()
		visited := collectWalk(t, bs, rootCid, walker.WithVisitedTracker(tracker))
		visitedSet := make(map[cid.Cid]struct{})
		for _, c := range visited {
			visitedSet[c] = struct{}{}
		}

		assert.Len(t, visitedSet, len(allCids),
			"WalkDAG must visit every unique block in the HAMT (internal shards + leaf entries)")
		for c := range allCids {
			assert.Contains(t, visitedSet, c,
				"CID %s reachable from HAMT root must be visited", c)
		}

		// verify internal shard nodes exist (not just leaves)
		internalCount := len(allCids) - len(leafCids)
		assert.Greater(t, internalCount, 1,
			"HAMT with 500 entries must have multiple internal shard nodes")
		t.Logf("HAMT: %d total blocks (%d internal shards, %d leaf entries)",
			len(allCids), internalCount, len(leafCids))
	})
}

func TestWalkDAG_Dedup(t *testing.T) {
	// MapTracker across two walks: CIDs from the first walk are
	// skipped in the second walk. This is the core mechanism for
	// cross-pin dedup in the reprovide cycle.
	t.Run("shared MapTracker skips already-visited subtrees", func(t *testing.T) {
		bs := newTestBlockstore()
		root1, cids1 := buildDAG(t, bs, 2, 2)
		root2, _ := buildDAG(t, bs, 2, 2)

		tracker := walker.NewMapTracker()

		visited1 := collectWalk(t, bs, root1, walker.WithVisitedTracker(tracker))
		assert.Len(t, visited1, len(cids1))

		// second walk: independent root, but if any CID overlapped
		// it would be skipped
		visited2 := collectWalk(t, bs, root2, walker.WithVisitedTracker(tracker))
		for _, c := range visited2 {
			for _, c1 := range visited1 {
				assert.NotEqual(t, c, c1,
					"CID from first walk must not appear in second walk")
			}
		}
	})

	// BloomTracker: walk the same root twice. Second walk should
	// produce zero CIDs because everything is already in the bloom.
	t.Run("shared BloomTracker dedup across walks of same root", func(t *testing.T) {
		bs := newTestBlockstore()
		root, allCids := buildDAG(t, bs, 3, 2)

		tracker, err := walker.NewBloomTracker(walker.MinBloomCapacity, walker.DefaultBloomFPRate)
		require.NoError(t, err)

		visited1 := collectWalk(t, bs, root, walker.WithVisitedTracker(tracker))
		assert.Len(t, visited1, len(allCids))

		visited2 := collectWalk(t, bs, root, walker.WithVisitedTracker(tracker))
		assert.Empty(t, visited2,
			"second walk of same root must produce zero CIDs")
	})
}

func TestWalkDAG_Locality(t *testing.T) {
	// WithLocality filters CIDs that are not locally available.
	// Used by MFS providers to skip blocks not in the local blockstore.
	t.Run("only local CIDs are visited", func(t *testing.T) {
		bs := newTestBlockstore()
		root, _ := buildDAG(t, bs, 2, 1)

		locality := func(_ context.Context, c cid.Cid) (bool, error) {
			return c == root, nil // only root is "local"
		}

		visited := collectWalk(t, bs, root, walker.WithLocality(locality))
		assert.Len(t, visited, 1,
			"only the local root should be visited")
		assert.Equal(t, root, visited[0])
	})

	// Locality errors should skip the CID (best-effort), not crash
	// the walk.
	t.Run("locality error skips CID gracefully", func(t *testing.T) {
		bs := newTestBlockstore()
		root, _ := buildDAG(t, bs, 2, 1)

		locality := func(_ context.Context, c cid.Cid) (bool, error) {
			if c == root {
				return true, nil
			}
			return false, assert.AnError
		}

		visited := collectWalk(t, bs, root, walker.WithLocality(locality))
		assert.Len(t, visited, 1,
			"children with locality errors should be skipped")
		assert.Equal(t, root, visited[0])
	})
}

func TestWalkDAG_ErrorHandling(t *testing.T) {
	// When the root itself is missing from the blockstore, the walk
	// should return successfully with zero CIDs (best-effort: a
	// corrupt block should not break the entire provide cycle).
	t.Run("missing root produces no CIDs", func(t *testing.T) {
		bs := newTestBlockstore()
		missing := putRawBlock(t, bs, []byte("will-be-deleted"))
		require.NoError(t, bs.DeleteBlock(t.Context(), missing))

		var visited []cid.Cid
		fetch := walker.LinksFetcherFromBlockstore(bs)
		err := walker.WalkDAG(t.Context(), missing, fetch, func(c cid.Cid) bool {
			visited = append(visited, c)
			return true
		})
		require.NoError(t, err,
			"walk should succeed even with missing root (best-effort)")
		assert.Empty(t, visited)
	})

	// When children fail to fetch, they are skipped but the root is
	// still emitted. This ensures a corrupt child block doesn't
	// prevent the parent from being provided.
	t.Run("fetch error on children skips them but emits root", func(t *testing.T) {
		bs := newTestBlockstore()
		root, _ := buildDAG(t, bs, 2, 1)

		realFetch := walker.LinksFetcherFromBlockstore(bs)
		failFetch := func(ctx context.Context, c cid.Cid) ([]cid.Cid, error) {
			if c == root {
				return realFetch(ctx, c)
			}
			return nil, assert.AnError
		}

		var visited []cid.Cid
		err := walker.WalkDAG(t.Context(), root, failFetch, func(c cid.Cid) bool {
			visited = append(visited, c)
			return true
		})
		require.NoError(t, err)
		assert.Len(t, visited, 1,
			"only root should be emitted when children fail")
		assert.Equal(t, root, visited[0])
	})

	// CIDs are marked visited at pop time (before fetch). If fetch
	// fails, the CID stays in the tracker and won't be retried this
	// cycle. This avoids a double bloom scan per CID. The CID is
	// caught in the next reprovide cycle (22h).
	t.Run("fetch error still marks CID as visited", func(t *testing.T) {
		bs := newTestBlockstore()
		root, _ := buildDAG(t, bs, 2, 1)

		tracker := walker.NewMapTracker()
		realFetch := walker.LinksFetcherFromBlockstore(bs)
		failChildFetch := func(ctx context.Context, c cid.Cid) ([]cid.Cid, error) {
			if c != root {
				return nil, assert.AnError
			}
			return realFetch(ctx, c)
		}

		walker.WalkDAG(t.Context(), root, failChildFetch, func(c cid.Cid) bool {
			return true
		}, walker.WithVisitedTracker(tracker))

		children, _ := realFetch(t.Context(), root)
		for _, child := range children {
			assert.True(t, tracker.Has(child),
				"CID %s must be marked visited even after fetch error", child)
		}
	})
}

func TestWalkDAG_StopConditions(t *testing.T) {
	// emit returning false must stop the walk immediately.
	t.Run("emit false stops walk after N CIDs", func(t *testing.T) {
		bs := newTestBlockstore()
		root, _ := buildDAG(t, bs, 3, 3)

		count := 0
		fetch := walker.LinksFetcherFromBlockstore(bs)
		err := walker.WalkDAG(t.Context(), root, fetch, func(c cid.Cid) bool {
			count++
			return count < 5
		})
		require.NoError(t, err)
		assert.Equal(t, 5, count,
			"walk should stop after emit returns false")
	})

	// Context cancellation during the walk should stop it and return
	// the context error.
	t.Run("context cancellation stops walk mid-flight", func(t *testing.T) {
		bs := newTestBlockstore()
		root, _ := buildDAG(t, bs, 3, 3)

		ctx, cancel := context.WithCancel(t.Context())
		count := 0
		fetch := walker.LinksFetcherFromBlockstore(bs)
		err := walker.WalkDAG(ctx, root, fetch, func(c cid.Cid) bool {
			count++
			if count >= 3 {
				cancel()
			}
			return true
		})
		assert.ErrorIs(t, err, context.Canceled)
	})

	// An already-cancelled context should return immediately without
	// visiting any CIDs.
	t.Run("already-cancelled context returns immediately", func(t *testing.T) {
		bs := newTestBlockstore()
		root, _ := buildDAG(t, bs, 2, 1)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		var visited []cid.Cid
		fetch := walker.LinksFetcherFromBlockstore(bs)
		err := walker.WalkDAG(ctx, root, fetch, func(c cid.Cid) bool {
			visited = append(visited, c)
			return true
		})
		assert.ErrorIs(t, err, context.Canceled)
		assert.Empty(t, visited,
			"no CIDs should be visited with cancelled context")
	})
}
