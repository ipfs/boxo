package walker_test

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
	"github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/ipld/unixfs/hamt"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	ipld "github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDAGService(bs blockstore.Blockstore) format.DAGService {
	bserv := blockservice.New(bs, offline.Exchange(bs))
	return merkledag.NewDAGService(bserv)
}

func fileNodeWithData(t *testing.T, data []byte) *merkledag.ProtoNode {
	t.Helper()
	fsn := ft.NewFSNode(ft.TFile)
	fsn.SetData(data)
	nodeData, err := fsn.GetBytes()
	require.NoError(t, err)
	return merkledag.NodeWithData(nodeData)
}

// putDagCBOR builds a dag-cbor block {name: string, links: [CID...]}
// and stores it in the blockstore. Returns its CID.
func putDagCBOR(t *testing.T, bs blockstore.Blockstore, name string, linkCIDs ...cid.Cid) cid.Cid {
	t.Helper()
	ls := cidlink.DefaultLinkSystem()
	ls.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.Buffer{}
		return &buf, func(lnk ipld.Link) error {
			cl := lnk.(cidlink.Link)
			blk, err := blocks.NewBlockWithCid(buf.Bytes(), cl.Cid)
			if err != nil {
				return err
			}
			return bs.Put(context.Background(), blk)
		}, nil
	}
	lp := cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version: 1, Codec: cid.DagCBOR, MhType: 0x12, MhLength: 32,
	}}
	nd, err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "name", qp.String(name))
		qp.MapEntry(ma, "links", qp.List(-1, func(la ipld.ListAssembler) {
			for _, c := range linkCIDs {
				qp.ListEntry(la, qp.Link(cidlink.Link{Cid: c}))
			}
		}))
	})
	require.NoError(t, err)
	lnk, err := ls.Store(ipld.LinkContext{}, lp, nd)
	require.NoError(t, err)
	return lnk.(cidlink.Link).Cid
}

func collectEntityWalk(t *testing.T, bs blockstore.Blockstore, root cid.Cid, opts ...walker.Option) []cid.Cid {
	t.Helper()
	var visited []cid.Cid
	fetch := walker.NodeFetcherFromBlockstore(bs)
	err := walker.WalkEntityRoots(t.Context(), root, fetch, func(c cid.Cid) bool {
		visited = append(visited, c)
		return true
	}, opts...)
	require.NoError(t, err)
	return visited
}

// --- UnixFS entity type detection ---
//
// These tests verify that WalkEntityRoots correctly identifies UnixFS
// entity types and treats them appropriately: files stop traversal
// (chunks not descended), directories and HAMT shards recurse.

func TestEntityWalk_UnixFSFile(t *testing.T) {
	// A single UnixFS file is an entity root. WalkEntityRoots should
	// emit it and stop -- no children to recurse into.
	bs := newTestBlockstore()
	dserv := newTestDAGService(bs)
	file := fileNodeWithData(t, []byte("content"))
	require.NoError(t, dserv.Add(t.Context(), file))

	visited := collectEntityWalk(t, bs, file.Cid())
	assert.Len(t, visited, 1)
	assert.Equal(t, file.Cid(), visited[0])
}

func TestEntityWalk_ChunkedFileDoesNotDescend(t *testing.T) {
	// A chunked file has child links (chunks). This is the core
	// +entities optimization: the file root is emitted but its chunks
	// are NOT traversed. Without this, every chunk CID would be
	// provided to the DHT, which is wasteful.
	bs := newTestBlockstore()
	dserv := newTestDAGService(bs)

	chunk1 := merkledag.NewRawNode([]byte("chunk1"))
	chunk2 := merkledag.NewRawNode([]byte("chunk2"))
	require.NoError(t, dserv.Add(t.Context(), chunk1))
	require.NoError(t, dserv.Add(t.Context(), chunk2))

	fsn := ft.NewFSNode(ft.TFile)
	fsn.AddBlockSize(6)
	fsn.AddBlockSize(6)
	fileData, err := fsn.GetBytes()
	require.NoError(t, err)
	fileNode := merkledag.NodeWithData(fileData)
	fileNode.AddNodeLink("", chunk1)
	fileNode.AddNodeLink("", chunk2)
	require.NoError(t, dserv.Add(t.Context(), fileNode))

	visited := collectEntityWalk(t, bs, fileNode.Cid())
	assert.Len(t, visited, 1, "only file root, NOT chunks")
	assert.Equal(t, fileNode.Cid(), visited[0])
	assert.NotContains(t, visited, chunk1.Cid())
	assert.NotContains(t, visited, chunk2.Cid())
}

func TestEntityWalk_RawNodeIsFile(t *testing.T) {
	// Raw codec blocks (CIDv1 small files) are treated as files.
	// They have no children and should be emitted as a single entity.
	bs := newTestBlockstore()
	raw := merkledag.NewRawNode([]byte("small file"))
	require.NoError(t, bs.Put(t.Context(), raw))

	visited := collectEntityWalk(t, bs, raw.Cid())
	assert.Len(t, visited, 1)
	assert.Equal(t, raw.Cid(), visited[0])
}

func TestEntityWalk_Directory(t *testing.T) {
	// A UnixFS directory is a container entity. It is emitted and its
	// children (files) are recursed into. Each file is also emitted.
	bs := newTestBlockstore()
	dserv := newTestDAGService(bs)

	file1 := fileNodeWithData(t, []byte("file1"))
	file2 := fileNodeWithData(t, []byte("file2"))
	require.NoError(t, dserv.Add(t.Context(), file1))
	require.NoError(t, dserv.Add(t.Context(), file2))

	dir := ft.EmptyDirNode()
	dir.AddNodeLink("file1", file1)
	dir.AddNodeLink("file2", file2)
	require.NoError(t, dserv.Add(t.Context(), dir))

	visited := collectEntityWalk(t, bs, dir.Cid())
	assert.Len(t, visited, 3, "dir + 2 files")
	assert.Equal(t, dir.Cid(), visited[0], "directory emitted first")
	assert.Contains(t, visited, file1.Cid())
	assert.Contains(t, visited, file2.Cid())
}

func TestEntityWalk_DirectoryWithRawFiles(t *testing.T) {
	// Directory containing raw codec files. Both the directory and the
	// raw files should be emitted (raw = file entity).
	bs := newTestBlockstore()
	dserv := newTestDAGService(bs)

	raw1 := merkledag.NewRawNode([]byte("raw 1"))
	raw2 := merkledag.NewRawNode([]byte("raw 2"))
	require.NoError(t, dserv.Add(t.Context(), raw1))
	require.NoError(t, dserv.Add(t.Context(), raw2))

	dir := ft.EmptyDirNode()
	dir.AddNodeLink("r1.bin", raw1)
	dir.AddNodeLink("r2.bin", raw2)
	require.NoError(t, dserv.Add(t.Context(), dir))

	visited := collectEntityWalk(t, bs, dir.Cid())
	assert.Len(t, visited, 3, "dir + 2 raw files")
	assert.Contains(t, visited, dir.Cid())
	assert.Contains(t, visited, raw1.Cid())
	assert.Contains(t, visited, raw2.Cid())
}

// --- HAMT sharded directories ---

func TestEntityWalk_HAMTDirectory(t *testing.T) {
	// HAMT sharded directories store entries across multiple internal
	// shard nodes. WalkEntityRoots must emit ALL shard nodes (needed
	// for peers to enumerate the directory) AND all file entries, but
	// must NOT descend into file chunks.
	bs := newTestBlockstore()
	dserv := newTestDAGService(bs)

	shard, err := hamt.NewShard(dserv, 256)
	require.NoError(t, err)

	numFiles := 100
	fileCIDs := make(map[cid.Cid]struct{})
	for i := range numFiles {
		file := fileNodeWithData(t, []byte(fmt.Sprintf("hamt-file-%d", i)))
		require.NoError(t, dserv.Add(t.Context(), file))
		require.NoError(t, shard.Set(t.Context(), fmt.Sprintf("file%d.txt", i), file))
		fileCIDs[file.Cid()] = struct{}{}
	}
	rootNd, err := shard.Node()
	require.NoError(t, err)

	tracker := walker.NewMapTracker()
	visited := collectEntityWalk(t, bs, rootNd.Cid(), walker.WithVisitedTracker(tracker))

	// all files emitted
	for fc := range fileCIDs {
		assert.Contains(t, visited, fc)
	}
	// root shard emitted
	assert.Contains(t, visited, rootNd.Cid())
	// internal shard nodes exist beyond just files + root
	shardCount := len(visited) - numFiles
	assert.Greater(t, shardCount, 0,
		"HAMT with %d entries must have internal shard nodes", numFiles)
	t.Logf("HAMT: %d visited (%d files + %d shard nodes)", len(visited), numFiles, shardCount)
}

// --- non-UnixFS codecs (dag-cbor) ---
//
// The +entities chunk-skip optimization only applies to UnixFS files.
// For all other codecs, every reachable CID is emitted AND its children
// are followed. This ensures that dag-cbor metadata wrapping UnixFS
// content is fully discoverable.

func TestEntityWalk_DagCBORStandalone(t *testing.T) {
	// A single dag-cbor block with no links. Should be emitted as an
	// opaque entity root (non-UnixFS).
	bs := newTestBlockstore()
	c := putDagCBOR(t, bs, "standalone")
	visited := collectEntityWalk(t, bs, c)
	assert.Len(t, visited, 1)
	assert.Equal(t, c, visited[0])
}

func TestEntityWalk_DagCBORChain(t *testing.T) {
	// dag-cbor A -> B -> C. All three are non-UnixFS, so all must be
	// emitted. The walk follows links in non-UnixFS nodes to discover
	// further content.
	bs := newTestBlockstore()
	cC := putDagCBOR(t, bs, "C")
	cB := putDagCBOR(t, bs, "B", cC)
	cA := putDagCBOR(t, bs, "A", cB)

	visited := collectEntityWalk(t, bs, cA)
	assert.Len(t, visited, 3, "all dag-cbor nodes emitted")
	assert.Contains(t, visited, cA)
	assert.Contains(t, visited, cB)
	assert.Contains(t, visited, cC)
}

func TestEntityWalk_DagCBORLinkingToUnixFS(t *testing.T) {
	// dag-cbor root linking to a chunked UnixFS file. The dag-cbor
	// root and the file root are emitted, but the file's chunks are
	// NOT (entities optimization applies to the UnixFS file).
	bs := newTestBlockstore()
	dserv := newTestDAGService(bs)

	chunk := merkledag.NewRawNode([]byte("chunk"))
	require.NoError(t, dserv.Add(t.Context(), chunk))
	fsn := ft.NewFSNode(ft.TFile)
	fsn.AddBlockSize(5)
	fileData, err := fsn.GetBytes()
	require.NoError(t, err)
	fileNode := merkledag.NodeWithData(fileData)
	fileNode.AddNodeLink("", chunk)
	require.NoError(t, dserv.Add(t.Context(), fileNode))

	cborRoot := putDagCBOR(t, bs, "metadata", fileNode.Cid())

	visited := collectEntityWalk(t, bs, cborRoot)
	assert.Contains(t, visited, cborRoot, "dag-cbor root emitted")
	assert.Contains(t, visited, fileNode.Cid(), "UnixFS file root emitted")
	assert.NotContains(t, visited, chunk.Cid(),
		"file chunks NOT emitted (entities optimization)")
	assert.Len(t, visited, 2)
}

// --- mixed codec DAG ---

func TestEntityWalk_MixedCodecs(t *testing.T) {
	// dag-cbor root -> UnixFS directory -> {file, raw leaf}
	// All entity roots emitted, file chunks skipped.
	bs := newTestBlockstore()
	dserv := newTestDAGService(bs)

	raw := merkledag.NewRawNode([]byte("raw leaf"))
	require.NoError(t, dserv.Add(t.Context(), raw))

	file := fileNodeWithData(t, []byte("file content"))
	require.NoError(t, dserv.Add(t.Context(), file))

	dir := ft.EmptyDirNode()
	dir.AddNodeLink("raw.bin", raw)
	dir.AddNodeLink("file.txt", file)
	require.NoError(t, dserv.Add(t.Context(), dir))

	cborRoot := putDagCBOR(t, bs, "wrapper", dir.Cid())

	visited := collectEntityWalk(t, bs, cborRoot)
	assert.Len(t, visited, 4, "cbor root + dir + file + raw")
	assert.Contains(t, visited, cborRoot)
	assert.Contains(t, visited, dir.Cid())
	assert.Contains(t, visited, file.Cid())
	assert.Contains(t, visited, raw.Cid())
}

// --- dedup across walks ---

func TestEntityWalk_SharedTrackerDedup(t *testing.T) {
	// Two directories sharing a file. With a shared VisitedTracker,
	// the shared file is emitted only once across both walks. This is
	// the cross-pin dedup mechanism for the reprovide cycle.
	bs := newTestBlockstore()
	dserv := newTestDAGService(bs)

	shared := fileNodeWithData(t, []byte("shared"))
	unique1 := fileNodeWithData(t, []byte("unique1"))
	unique2 := fileNodeWithData(t, []byte("unique2"))
	require.NoError(t, dserv.Add(t.Context(), shared))
	require.NoError(t, dserv.Add(t.Context(), unique1))
	require.NoError(t, dserv.Add(t.Context(), unique2))

	dir1 := ft.EmptyDirNode()
	dir1.AddNodeLink("shared", shared)
	dir1.AddNodeLink("unique", unique1)
	require.NoError(t, dserv.Add(t.Context(), dir1))

	dir2 := ft.EmptyDirNode()
	dir2.AddNodeLink("shared", shared)
	dir2.AddNodeLink("unique", unique2)
	require.NoError(t, dserv.Add(t.Context(), dir2))

	tracker := walker.NewMapTracker()
	fetch := walker.NodeFetcherFromBlockstore(bs)
	var all []cid.Cid

	walker.WalkEntityRoots(t.Context(), dir1.Cid(), fetch, func(c cid.Cid) bool {
		all = append(all, c)
		return true
	}, walker.WithVisitedTracker(tracker))

	walker.WalkEntityRoots(t.Context(), dir2.Cid(), fetch, func(c cid.Cid) bool {
		all = append(all, c)
		return true
	}, walker.WithVisitedTracker(tracker))

	// dir1 + shared + unique1 + dir2 + unique2 = 5
	assert.Len(t, all, 5)
	sharedCount := 0
	for _, c := range all {
		if c == shared.Cid() {
			sharedCount++
		}
	}
	assert.Equal(t, 1, sharedCount, "shared file emitted only once")
}

// --- stop conditions ---

func TestEntityWalk_EmitFalseStops(t *testing.T) {
	// Returning false from emit must stop the walk immediately.
	// Important for callers that want to limit results.
	bs := newTestBlockstore()
	dserv := newTestDAGService(bs)

	dir := ft.EmptyDirNode()
	for i := range 5 {
		f := fileNodeWithData(t, []byte(fmt.Sprintf("f%d", i)))
		require.NoError(t, dserv.Add(t.Context(), f))
		dir.AddNodeLink(fmt.Sprintf("f%d", i), f)
	}
	require.NoError(t, dserv.Add(t.Context(), dir))

	count := 0
	fetch := walker.NodeFetcherFromBlockstore(bs)
	walker.WalkEntityRoots(t.Context(), dir.Cid(), fetch, func(c cid.Cid) bool {
		count++
		return count < 3
	})
	assert.Equal(t, 3, count)
}

func TestEntityWalk_ContextCancellation(t *testing.T) {
	// Walk must respect context cancellation and return the error.
	bs := newTestBlockstore()
	dserv := newTestDAGService(bs)

	file := fileNodeWithData(t, []byte("file"))
	require.NoError(t, dserv.Add(t.Context(), file))
	dir := ft.EmptyDirNode()
	dir.AddNodeLink("file", file)
	require.NoError(t, dserv.Add(t.Context(), dir))

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel before walk starts

	fetch := walker.NodeFetcherFromBlockstore(bs)
	err := walker.WalkEntityRoots(ctx, dir.Cid(), fetch, func(c cid.Cid) bool {
		return true
	})
	assert.ErrorIs(t, err, context.Canceled)
}

// --- error handling ---

func TestEntityWalk_FetchErrorSkips(t *testing.T) {
	// Missing child blocks are skipped gracefully (best-effort).
	// The walk continues with other branches. This prevents a single
	// corrupt block from breaking the entire provide cycle.
	bs := newTestBlockstore()
	dserv := newTestDAGService(bs)

	exists := fileNodeWithData(t, []byte("exists"))
	require.NoError(t, dserv.Add(t.Context(), exists))

	missing := fileNodeWithData(t, []byte("missing"))
	// intentionally NOT added to blockstore

	dir := ft.EmptyDirNode()
	dir.AddNodeLink("exists", exists)
	dir.AddNodeLink("missing", missing)
	require.NoError(t, dserv.Add(t.Context(), dir))

	visited := collectEntityWalk(t, bs, dir.Cid())
	assert.Contains(t, visited, dir.Cid())
	assert.Contains(t, visited, exists.Cid())
	assert.NotContains(t, visited, missing.Cid(),
		"missing block should be skipped, not crash")
}
