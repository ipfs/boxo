package walker_test

// Tests verifying that identity CIDs (multihash 0x00) are handled
// correctly across all walker and provider paths. Identity CIDs embed
// data inline, so providing them to the DHT is wasteful. The walker
// traverses through them (following links) but never emits them.
//
// This covers:
//   - WalkDAG: identity root, identity child, identity dag-pb dir
//   - WalkEntityRoots: identity file, identity dir, mixed normal+identity
//   - IsIdentityCID: predicate correctness

import (
	"testing"

	"github.com/ipfs/boxo/ipld/merkledag"
	mdtest "github.com/ipfs/boxo/ipld/merkledag/test"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	cid "github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeIdentityCID creates a CIDv1 with an identity multihash (data
// inline in the CID). codec determines the CID codec prefix.
func makeIdentityCID(t *testing.T, data []byte, codec uint64) cid.Cid {
	t.Helper()
	hash, err := mh.Encode(data, mh.IDENTITY)
	require.NoError(t, err)
	return cid.NewCidV1(codec, hash)
}

// --- WalkDAG identity tests ---

func TestWalkDAG_IdentityCID(t *testing.T) {
	t.Run("identity raw CID as root is not emitted", func(t *testing.T) {
		bs := newTestBlockstore()
		idCid := makeIdentityCID(t, []byte("inline"), cid.Raw)

		visited := collectWalk(t, bs, idCid)
		assert.Empty(t, visited,
			"identity CID root must not be emitted")
	})

	t.Run("dag-pb linking to identity child skips identity", func(t *testing.T) {
		bs := newTestBlockstore()
		dserv := merkledag.NewDAGService(mdtest.Bserv())

		idChild := makeIdentityCID(t, []byte("inline-child"), cid.Raw)

		root := merkledag.NodeWithData([]byte("root"))
		require.NoError(t, root.AddRawLink("inline", &format.Link{Cid: idChild}))
		require.NoError(t, dserv.Add(t.Context(), root))
		require.NoError(t, bs.Put(t.Context(), root))

		visited := collectWalk(t, bs, root.Cid())
		assert.Len(t, visited, 1, "only the non-identity root")
		assert.Equal(t, root.Cid(), visited[0])
		assert.NotContains(t, visited, idChild,
			"identity child must not be emitted")
	})

	t.Run("identity dag-pb directory with normal raw child", func(t *testing.T) {
		// simulates `ipfs add --inline` producing a small dag-pb
		// directory with identity multihash, linking to a normal
		// raw block. The identity directory must not be emitted,
		// but its normal child must be.
		bs := newTestBlockstore()

		normalChild := putRawBlock(t, bs, []byte("normal-data"))

		// build a dag-pb directory node
		dir := ft.EmptyDirNode()
		require.NoError(t, dir.AddRawLink("child.bin", &format.Link{Cid: normalChild}))

		// re-encode the directory with an identity multihash to
		// simulate what ipfs add --inline produces for small dirs
		dirData := dir.RawData()
		idHash, err := mh.Encode(dirData, mh.IDENTITY)
		require.NoError(t, err)
		idDirCid := cid.NewCidV1(cid.DagProtobuf, idHash)
		// NOT stored in blockstore -- NewIdStore decodes from CID

		visited := collectWalk(t, bs, idDirCid)
		assert.Len(t, visited, 1, "only the normal raw child")
		assert.Equal(t, normalChild, visited[0],
			"normal child reachable through identity dir must be emitted")
		assert.NotContains(t, visited, idDirCid,
			"identity directory must not be emitted")
	})
}

// --- WalkEntityRoots identity tests ---

func TestEntityWalk_IdentityFileNotEmitted(t *testing.T) {
	bs := newTestBlockstore()
	idFile := makeIdentityCID(t, []byte("tiny"), cid.Raw)

	visited := collectEntityWalk(t, bs, idFile)
	assert.Empty(t, visited, "identity file must not be emitted")
}

func TestEntityWalk_IdentityDirWithNormalChildren(t *testing.T) {
	// An identity dag-pb directory (like `ipfs add --inline` produces
	// for small dirs) linking to normal files. The identity directory
	// must not be emitted, but its normal children must be.
	bs := newTestBlockstore()
	dserv := newTestDAGService(bs)

	file1 := fileNodeWithData(t, []byte("file1"))
	file2 := fileNodeWithData(t, []byte("file2"))
	require.NoError(t, dserv.Add(t.Context(), file1))
	require.NoError(t, dserv.Add(t.Context(), file2))

	dir := ft.EmptyDirNode()
	dir.AddNodeLink("f1.txt", file1)
	dir.AddNodeLink("f2.txt", file2)

	// re-encode with identity multihash
	dirData := dir.RawData()
	idHash, err := mh.Encode(dirData, mh.IDENTITY)
	require.NoError(t, err)
	idDirCid := cid.NewCidV1(cid.DagProtobuf, idHash)

	visited := collectEntityWalk(t, bs, idDirCid)
	assert.Len(t, visited, 2, "both normal files emitted")
	assert.Contains(t, visited, file1.Cid())
	assert.Contains(t, visited, file2.Cid())
	assert.NotContains(t, visited, idDirCid,
		"identity directory must not be emitted")
}

func TestEntityWalk_NormalDirWithIdentityChild(t *testing.T) {
	// A normal directory containing an identity CID child. The
	// directory is emitted, the identity child is not.
	bs := newTestBlockstore()
	dserv := newTestDAGService(bs)

	idChild := makeIdentityCID(t, []byte("inline-file"), cid.Raw)

	normalFile := fileNodeWithData(t, []byte("normal"))
	require.NoError(t, dserv.Add(t.Context(), normalFile))

	dir := ft.EmptyDirNode()
	require.NoError(t, dir.AddRawLink("inline.bin", &format.Link{Cid: idChild}))
	dir.AddNodeLink("normal.txt", normalFile)
	require.NoError(t, dserv.Add(t.Context(), dir))

	visited := collectEntityWalk(t, bs, dir.Cid())
	assert.Contains(t, visited, dir.Cid(), "normal directory emitted")
	assert.Contains(t, visited, normalFile.Cid(), "normal file emitted")
	assert.NotContains(t, visited, idChild,
		"identity child must not be emitted")
}
