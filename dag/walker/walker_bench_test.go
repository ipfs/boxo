package walker_test

// Benchmarks comparing boxo/dag/walker against the legacy ipld-prime
// selector-based traversal (fetcherhelpers.BlockAll).
//
// Legacy path (kubo Provide.Strategy=pinned today):
//
//	bsfetcher with dagpb.AddSupportToChooser -> BlockAll selector traversal
//	(uses OfflineIPLDFetcher -- NO unixfsnode.Reify, that's MFS-only)
//
// New path:
//
//	WalkDAG + LinksFetcherFromBlockstore -> iterative DFS with VisitedTracker
//	(uses cidlink.DefaultLinkSystem from the blockstore)
//
// # Why the new walker is faster (even without dedup)
//
// The WalkerNoTracker variant has no bloom/map overhead yet is still
// ~2x faster than BlockAll. The speedup comes from architectural
// differences, not deduplication:
//
//   - No selector overhead: BlockAll constructs and interprets an
//     ipld-prime selector (ExploreRecursive + ExploreAll) for every
//     traversal. This involves building selector nodes, matching them
//     against the data model at each step, and maintaining selector
//     state across recursion levels. WalkDAG uses a plain stack-based
//     DFS with zero selector machinery.
//   - Simpler node decoding: BlockAll goes through bsfetcher which
//     wraps blockservice, creates per-session fetchers, and resolves
//     nodes via the full ipld-prime linking/loading pipeline with
//     prototype choosers. WalkDAG uses cidlink.DefaultLinkSystem
//     directly against the blockstore, skipping the blockservice and
//     session layers entirely.
//   - Fewer allocations: the selector path allocates FetchResult
//     structs, selector state, and intermediate node wrappers per
//     visited node. WalkDAG allocates only a CID slice per node
//     (the child links) and reuses the stack. This shows in the
//     benchmarks as ~30-40% fewer allocs/op.
//
// Dedup (bloom/map) adds a small overhead (~8-15%) on single walks
// but pays off across multiple walks of overlapping DAGs, which is
// the primary use case in reprovide cycles.
//
// # DAG types benchmarked
//
//   - dag-pb: standard UnixFS DAGs (most common pinned content)
//   - dag-cbor: IPLD-native DAGs (supported by pinned strategy, not UnixFS)
//   - mixed: dag-cbor root linking to dag-pb subtrees (realistic for
//     applications that wrap UnixFS content in dag-cbor metadata)
//
// # Variants per DAG type
//
//   - BlockAll: legacy ipld-prime selector traversal (baseline)
//   - WalkerNoTracker: new walker without dedup (pure walk cost)
//   - WalkerMapTracker: new walker with exact map-based dedup
//   - WalkerBloomTracker: new walker with bloom filter dedup
//   - BloomSecondWalk: bloom Has() skip speed when all CIDs already
//     in bloom (simulates cross-pin shared subtree skipping)

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/dag/walker"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/fetcher"
	fetcherhelpers "github.com/ipfs/boxo/fetcher/helpers"
	bsfetcher "github.com/ipfs/boxo/fetcher/impl/blockservice"
	"github.com/ipfs/boxo/ipld/merkledag"
	mdtest "github.com/ipfs/boxo/ipld/merkledag/test"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	dagpb "github.com/ipld/go-codec-dagpb"
	ipld "github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

type benchDAG struct {
	bs       blockstore.Blockstore
	bserv    blockservice.BlockService
	root     cid.Cid
	numNodes int
	fetchFac fetcher.Factory // matches kubo's OfflineIPLDFetcher
}

// makeBenchDAG_DagPB creates a pure dag-pb tree.
func makeBenchDAG_DagPB(b *testing.B, fanout, depth uint) *benchDAG {
	b.Helper()
	store := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	bserv := blockservice.New(store, offline.Exchange(store))
	dserv := merkledag.NewDAGService(bserv)
	gen := mdtest.NewDAGGenerator()
	root, allCids, err := gen.MakeDagNode(dserv.Add, fanout, depth)
	if err != nil {
		b.Fatal(err)
	}
	return &benchDAG{
		bs:       store,
		bserv:    bserv,
		root:     root,
		numNodes: len(allCids),
		fetchFac: makeIPLDFetcher(bserv),
	}
}

// makeBenchDAG_DagCBOR creates a pure dag-cbor tree where each node
// is a map with "data" (bytes) and "links" (list of CID links).
func makeBenchDAG_DagCBOR(b *testing.B, fanout, depth int) *benchDAG {
	b.Helper()
	store := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	bserv := blockservice.New(store, offline.Exchange(store))

	ls := cidlink.DefaultLinkSystem()
	ls.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.Buffer{}
		return &buf, func(lnk ipld.Link) error {
			cl := lnk.(cidlink.Link)
			blk, err := blocks.NewBlockWithCid(buf.Bytes(), cl.Cid)
			if err != nil {
				return err
			}
			return store.Put(context.Background(), blk)
		}, nil
	}

	lp := cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1,
		Codec:    cid.DagCBOR,
		MhType:   0x12, // sha2-256
		MhLength: 32,
	}}

	count := 0
	var build func(d int) ipld.Link
	build = func(d int) ipld.Link {
		var childLinks []ipld.Link
		if d < depth {
			for range fanout {
				childLinks = append(childLinks, build(d+1))
			}
		}
		nd, err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma ipld.MapAssembler) {
			qp.MapEntry(ma, "data", qp.Bytes(fmt.Appendf(nil, "node-%d", count)))
			qp.MapEntry(ma, "links", qp.List(-1, func(la ipld.ListAssembler) {
				for _, cl := range childLinks {
					qp.ListEntry(la, qp.Link(cl))
				}
			}))
		})
		if err != nil {
			b.Fatal(err)
		}
		lnk, err := ls.Store(ipld.LinkContext{}, lp, nd)
		if err != nil {
			b.Fatal(err)
		}
		count++
		return lnk
	}

	rootLink := build(0)
	rootCid := rootLink.(cidlink.Link).Cid

	return &benchDAG{
		bs:       store,
		bserv:    bserv,
		root:     rootCid,
		numNodes: count,
		fetchFac: makeIPLDFetcher(bserv),
	}
}

// makeBenchDAG_Mixed creates a dag-cbor root that links to multiple
// dag-pb subtrees (realistic: dag-cbor metadata wrapping UnixFS content).
func makeBenchDAG_Mixed(b *testing.B, dagPBFanout, dagPBDepth uint, numPBSubtrees int) *benchDAG {
	b.Helper()
	store := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	bserv := blockservice.New(store, offline.Exchange(store))
	dserv := merkledag.NewDAGService(bserv)

	// build dag-pb subtrees
	gen := mdtest.NewDAGGenerator()
	var subtreeRoots []cid.Cid
	totalNodes := 0
	for range numPBSubtrees {
		root, allCids, err := gen.MakeDagNode(dserv.Add, dagPBFanout, dagPBDepth)
		if err != nil {
			b.Fatal(err)
		}
		subtreeRoots = append(subtreeRoots, root)
		totalNodes += len(allCids)
	}

	// build dag-cbor root linking to all dag-pb subtrees
	ls := cidlink.DefaultLinkSystem()
	ls.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.Buffer{}
		return &buf, func(lnk ipld.Link) error {
			cl := lnk.(cidlink.Link)
			blk, err := blocks.NewBlockWithCid(buf.Bytes(), cl.Cid)
			if err != nil {
				return err
			}
			return store.Put(context.Background(), blk)
		}, nil
	}
	lp := cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1,
		Codec:    cid.DagCBOR,
		MhType:   0x12,
		MhLength: 32,
	}}

	nd, err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "type", qp.String("metadata"))
		qp.MapEntry(ma, "subtrees", qp.List(-1, func(la ipld.ListAssembler) {
			for _, r := range subtreeRoots {
				qp.ListEntry(la, qp.Link(cidlink.Link{Cid: r}))
			}
		}))
	})
	if err != nil {
		b.Fatal(err)
	}
	rootLink, err := ls.Store(ipld.LinkContext{}, lp, nd)
	if err != nil {
		b.Fatal(err)
	}
	totalNodes++ // the dag-cbor root itself

	return &benchDAG{
		bs:       store,
		bserv:    bserv,
		root:     rootLink.(cidlink.Link).Cid,
		numNodes: totalNodes,
		fetchFac: makeIPLDFetcher(bserv),
	}
}

// makeIPLDFetcher matches kubo's OfflineIPLDFetcher setup:
// dagpb prototype chooser, SkipNotFound, NO unixfsnode.Reify.
func makeIPLDFetcher(bserv blockservice.BlockService) fetcher.Factory {
	f := bsfetcher.NewFetcherConfig(bserv)
	f.SkipNotFound = true
	f.PrototypeChooser = dagpb.AddSupportToChooser(bsfetcher.DefaultPrototypeChooser)
	return f
}

// --- walk functions ---

func benchBlockAll(b *testing.B, dag *benchDAG) {
	b.Helper()
	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		session := dag.fetchFac.NewSession(ctx)
		err := fetcherhelpers.BlockAll(ctx, session, cidlink.Link{Cid: dag.root}, func(res fetcher.FetchResult) error {
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchWalkerNoTracker(b *testing.B, dag *benchDAG) {
	b.Helper()
	ctx := context.Background()
	fetch := walker.LinksFetcherFromBlockstore(dag.bs)
	b.ResetTimer()
	for range b.N {
		walker.WalkDAG(ctx, dag.root, fetch, func(c cid.Cid) bool { return true })
	}
}

func benchWalkerMapTracker(b *testing.B, dag *benchDAG) {
	b.Helper()
	ctx := context.Background()
	fetch := walker.LinksFetcherFromBlockstore(dag.bs)
	b.ResetTimer()
	for range b.N {
		t := walker.NewMapTracker()
		walker.WalkDAG(ctx, dag.root, fetch, func(c cid.Cid) bool { return true }, walker.WithVisitedTracker(t))
	}
}

func benchWalkerBloomTracker(b *testing.B, dag *benchDAG) {
	b.Helper()
	ctx := context.Background()
	fetch := walker.LinksFetcherFromBlockstore(dag.bs)
	b.ResetTimer()
	for range b.N {
		t, _ := walker.NewBloomTracker(max(uint(dag.numNodes), walker.MinBloomCapacity), walker.DefaultBloomFPRate)
		walker.WalkDAG(ctx, dag.root, fetch, func(c cid.Cid) bool { return true }, walker.WithVisitedTracker(t))
	}
}

func benchWalkerBloomSecondWalk(b *testing.B, dag *benchDAG) {
	b.Helper()
	ctx := context.Background()
	fetch := walker.LinksFetcherFromBlockstore(dag.bs)
	b.ResetTimer()
	for range b.N {
		t, _ := walker.NewBloomTracker(max(uint(dag.numNodes), walker.MinBloomCapacity), walker.DefaultBloomFPRate)
		walker.WalkDAG(ctx, dag.root, fetch, func(c cid.Cid) bool { return true }, walker.WithVisitedTracker(t))
		walker.WalkDAG(ctx, dag.root, fetch, func(c cid.Cid) bool {
			b.Fatal("unexpected CID in second walk")
			return false
		}, walker.WithVisitedTracker(t))
	}
}

// --- dag-pb benchmarks (fanout=10, depth=3 -> ~1111 nodes) ---

func BenchmarkDagPB_BlockAll(b *testing.B) { benchBlockAll(b, makeBenchDAG_DagPB(b, 10, 3)) }

func BenchmarkDagPB_WalkerNoTracker(b *testing.B) {
	benchWalkerNoTracker(b, makeBenchDAG_DagPB(b, 10, 3))
}

func BenchmarkDagPB_WalkerMapTracker(b *testing.B) {
	benchWalkerMapTracker(b, makeBenchDAG_DagPB(b, 10, 3))
}

func BenchmarkDagPB_WalkerBloomTracker(b *testing.B) {
	benchWalkerBloomTracker(b, makeBenchDAG_DagPB(b, 10, 3))
}

func BenchmarkDagPB_BloomSecondWalk(b *testing.B) {
	benchWalkerBloomSecondWalk(b, makeBenchDAG_DagPB(b, 10, 3))
}

// --- dag-cbor benchmarks (fanout=10, depth=3 -> 1111 nodes) ---

func BenchmarkDagCBOR_BlockAll(b *testing.B) { benchBlockAll(b, makeBenchDAG_DagCBOR(b, 10, 3)) }

func BenchmarkDagCBOR_WalkerNoTracker(b *testing.B) {
	benchWalkerNoTracker(b, makeBenchDAG_DagCBOR(b, 10, 3))
}

func BenchmarkDagCBOR_WalkerMapTracker(b *testing.B) {
	benchWalkerMapTracker(b, makeBenchDAG_DagCBOR(b, 10, 3))
}

func BenchmarkDagCBOR_WalkerBloomTracker(b *testing.B) {
	benchWalkerBloomTracker(b, makeBenchDAG_DagCBOR(b, 10, 3))
}

func BenchmarkDagCBOR_BloomSecondWalk(b *testing.B) {
	benchWalkerBloomSecondWalk(b, makeBenchDAG_DagCBOR(b, 10, 3))
}

// --- mixed benchmarks (dag-cbor root -> 5 dag-pb subtrees, each fanout=5 depth=2 -> ~156 nodes) ---

func BenchmarkMixed_BlockAll(b *testing.B) { benchBlockAll(b, makeBenchDAG_Mixed(b, 5, 2, 5)) }

func BenchmarkMixed_WalkerNoTracker(b *testing.B) {
	benchWalkerNoTracker(b, makeBenchDAG_Mixed(b, 5, 2, 5))
}

func BenchmarkMixed_WalkerMapTracker(b *testing.B) {
	benchWalkerMapTracker(b, makeBenchDAG_Mixed(b, 5, 2, 5))
}

func BenchmarkMixed_WalkerBloomTracker(b *testing.B) {
	benchWalkerBloomTracker(b, makeBenchDAG_Mixed(b, 5, 2, 5))
}

func BenchmarkMixed_BloomSecondWalk(b *testing.B) {
	benchWalkerBloomSecondWalk(b, makeBenchDAG_Mixed(b, 5, 2, 5))
}
