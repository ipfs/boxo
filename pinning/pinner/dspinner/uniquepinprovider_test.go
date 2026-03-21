package dspinner

import (
	"testing"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/dag/walker"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	mdutils "github.com/ipfs/boxo/ipld/merkledag/test"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	ipfspinner "github.com/ipfs/boxo/pinning/pinner"
	"github.com/ipfs/boxo/provider"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	format "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupPinTest(t *testing.T) (blockstore.Blockstore, ipfspinner.Pinner, format.DAGService) {
	t.Helper()
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bserv := blockservice.New(bs, offline.Exchange(bs))
	dserv := merkledag.NewDAGService(bserv)
	pinner, err := New(t.Context(), ds, dserv)
	require.NoError(t, err)
	return bs, pinner, dserv
}

// TestUniquePinnedProvider_DedupAcrossPins verifies that blocks shared
// between two recursive pins are emitted only once. This is the core
// use case: append-only datasets where each pin is the previous one
// plus a small delta, sharing the majority of their DAGs.
func TestUniquePinnedProvider_DedupAcrossPins(t *testing.T) {
	bs, pinner, dserv := setupPinTest(t)

	// two DAGs that share a common subtree
	shared := merkledag.NodeWithData([]byte("shared"))
	require.NoError(t, dserv.Add(t.Context(), shared))

	root1 := merkledag.NodeWithData([]byte("root1"))
	root1.AddNodeLink("shared", shared)
	require.NoError(t, dserv.Add(t.Context(), root1))

	root2 := merkledag.NodeWithData([]byte("root2"))
	root2.AddNodeLink("shared", shared)
	require.NoError(t, dserv.Add(t.Context(), root2))

	require.NoError(t, pinner.PinWithMode(t.Context(), root1.Cid(), ipfspinner.Recursive, "pin1"))
	require.NoError(t, pinner.PinWithMode(t.Context(), root2.Cid(), ipfspinner.Recursive, "pin2"))

	tracker := walker.NewMapTracker()
	keyChanF := NewUniquePinnedProvider(pinner, bs, tracker)
	ch, err := keyChanF(t.Context())
	require.NoError(t, err)

	visited := make(map[cid.Cid]int)
	for c := range ch {
		visited[c]++
	}

	assert.Equal(t, 1, visited[shared.Cid()],
		"shared block emitted exactly once across both pins")
	assert.Equal(t, 1, visited[root1.Cid()])
	assert.Equal(t, 1, visited[root2.Cid()])
	assert.Len(t, visited, 3, "root1 + root2 + shared")
}

// TestUniquePinnedProvider_DirectPins verifies that direct pins are
// emitted and deduplicated against recursive pin walks. A CID that
// appears both as a direct pin and within a recursive pin DAG should
// be emitted only once.
func TestUniquePinnedProvider_DirectPins(t *testing.T) {
	bs, pinner, dserv := setupPinTest(t)

	leaf := merkledag.NodeWithData([]byte("leaf"))
	require.NoError(t, dserv.Add(t.Context(), leaf))

	root := merkledag.NodeWithData([]byte("root"))
	root.AddNodeLink("leaf", leaf)
	require.NoError(t, dserv.Add(t.Context(), root))

	// pin root recursively (covers root + leaf)
	require.NoError(t, pinner.PinWithMode(t.Context(), root.Cid(), ipfspinner.Recursive, "rec"))
	// also direct-pin the leaf
	require.NoError(t, pinner.PinWithMode(t.Context(), leaf.Cid(), ipfspinner.Direct, "dir"))

	tracker := walker.NewMapTracker()
	keyChanF := NewUniquePinnedProvider(pinner, bs, tracker)
	ch, err := keyChanF(t.Context())
	require.NoError(t, err)

	visited := make(map[cid.Cid]int)
	for c := range ch {
		visited[c]++
	}

	assert.Equal(t, 1, visited[leaf.Cid()],
		"leaf emitted once despite being both recursively and directly pinned")
	assert.Equal(t, 1, visited[root.Cid()])
	assert.Len(t, visited, 2)
}

// TestUniquePinnedProvider_BufferedProviderCompat verifies that
// NewUniquePinnedProvider works with NewBufferedProvider, matching
// the wrapping pattern used in kubo's createKeyProvider.
func TestUniquePinnedProvider_BufferedProviderCompat(t *testing.T) {
	bs, pinner, dserv := setupPinTest(t)
	daggen := mdutils.NewDAGGenerator()
	root, allCids, err := daggen.MakeDagNode(dserv.Add, 3, 2)
	require.NoError(t, err)
	require.NoError(t, pinner.PinWithMode(t.Context(), root, ipfspinner.Recursive, "test"))

	tracker := walker.NewMapTracker()
	keyChanF := provider.NewBufferedProvider(
		NewUniquePinnedProvider(pinner, bs, tracker))
	ch, err := keyChanF(t.Context())
	require.NoError(t, err)

	count := 0
	for range ch {
		count++
	}
	assert.Equal(t, len(allCids), count)
}

// TestPinnedEntityRootsProvider_SkipsChunks verifies that the entity
// roots provider emits file roots but does not descend into chunks.
// This is the core optimization of the +entities strategy applied to
// pinned content.
func TestPinnedEntityRootsProvider_SkipsChunks(t *testing.T) {
	bs, pinner, dserv := setupPinTest(t)

	// chunked file: root -> chunk1, chunk2
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

	// directory containing the file
	dir := ft.EmptyDirNode()
	dir.AddNodeLink("big.bin", fileNode)
	require.NoError(t, dserv.Add(t.Context(), dir))
	require.NoError(t, pinner.PinWithMode(t.Context(), dir.Cid(), ipfspinner.Recursive, "test"))

	tracker := walker.NewMapTracker()
	keyChanF := NewPinnedEntityRootsProvider(pinner, bs, tracker)
	ch, err := keyChanF(t.Context())
	require.NoError(t, err)

	var visited []cid.Cid
	for c := range ch {
		visited = append(visited, c)
	}

	assert.Contains(t, visited, dir.Cid(), "directory emitted")
	assert.Contains(t, visited, fileNode.Cid(), "file root emitted")
	assert.NotContains(t, visited, chunk1.Cid(), "chunk1 NOT emitted")
	assert.NotContains(t, visited, chunk2.Cid(), "chunk2 NOT emitted")
	assert.Len(t, visited, 2, "dir + file root only")
}

// TestPinnedEntityRootsProvider_DedupAcrossPins verifies that entity
// roots shared between pins are emitted only once, same as
// NewUniquePinnedProvider but at the entity level.
func TestPinnedEntityRootsProvider_DedupAcrossPins(t *testing.T) {
	bs, pinner, dserv := setupPinTest(t)

	// shared file across two directories
	sharedFile := merkledag.NodeWithData(func() []byte {
		fsn := ft.NewFSNode(ft.TFile)
		fsn.SetData([]byte("shared"))
		b, _ := fsn.GetBytes()
		return b
	}())
	require.NoError(t, dserv.Add(t.Context(), sharedFile))

	// directories must be distinct (different unique files) so they
	// get different CIDs
	unique1 := merkledag.NodeWithData(func() []byte {
		fsn := ft.NewFSNode(ft.TFile)
		fsn.SetData([]byte("unique1"))
		b, _ := fsn.GetBytes()
		return b
	}())
	unique2 := merkledag.NodeWithData(func() []byte {
		fsn := ft.NewFSNode(ft.TFile)
		fsn.SetData([]byte("unique2"))
		b, _ := fsn.GetBytes()
		return b
	}())
	require.NoError(t, dserv.Add(t.Context(), unique1))
	require.NoError(t, dserv.Add(t.Context(), unique2))

	dir1 := ft.EmptyDirNode()
	dir1.AddNodeLink("shared.txt", sharedFile)
	dir1.AddNodeLink("unique.txt", unique1)
	require.NoError(t, dserv.Add(t.Context(), dir1))

	dir2 := ft.EmptyDirNode()
	dir2.AddNodeLink("shared.txt", sharedFile)
	dir2.AddNodeLink("unique.txt", unique2)
	require.NoError(t, dserv.Add(t.Context(), dir2))

	require.NoError(t, pinner.PinWithMode(t.Context(), dir1.Cid(), ipfspinner.Recursive, "pin1"))
	require.NoError(t, pinner.PinWithMode(t.Context(), dir2.Cid(), ipfspinner.Recursive, "pin2"))

	tracker := walker.NewMapTracker()
	keyChanF := NewPinnedEntityRootsProvider(pinner, bs, tracker)
	ch, err := keyChanF(t.Context())
	require.NoError(t, err)

	visited := make(map[cid.Cid]int)
	for c := range ch {
		visited[c]++
	}

	assert.Equal(t, 1, visited[sharedFile.Cid()],
		"shared file emitted once across both pins")
	assert.Len(t, visited, 5, "dir1 + dir2 + shared + unique1 + unique2")
}
