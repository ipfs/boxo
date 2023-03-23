package car

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/boxo/ipld/car/v2/index"
	"github.com/ipfs/boxo/ipld/car/v2/internal/carv1"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/boxo/ipld/merkledag"
	dstest "github.com/ipfs/boxo/ipld/merkledag/test"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/assert"
)

func TestWrapV1(t *testing.T) {
	// Produce a CARv1 file to test wrapping with.
	dagSvc := dstest.Mock()
	src := filepath.Join(t.TempDir(), "unwrapped-test-v1.car")
	sf, err := os.Create(src)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sf.Close()) })
	require.NoError(t, carv1.WriteCar(context.Background(), dagSvc, generateRootCid(t, dagSvc), sf))

	// Wrap the test CARv1 file
	dest := filepath.Join(t.TempDir(), "wrapped-test-v1.car")
	df, err := os.Create(dest)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, df.Close()) })
	_, err = sf.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.NoError(t, WrapV1(sf, df))

	// Assert wrapped file is valid CARv2 with CARv1 data payload matching the original CARv1 file.
	subject, err := OpenReader(dest)
	t.Cleanup(func() { require.NoError(t, subject.Close()) })
	require.NoError(t, err)

	// Assert CARv1 data payloads are identical.
	_, err = sf.Seek(0, io.SeekStart)
	require.NoError(t, err)
	wantPayload, err := io.ReadAll(sf)
	require.NoError(t, err)
	dr, err := subject.DataReader()
	require.NoError(t, err)
	gotPayload, err := io.ReadAll(dr)
	require.NoError(t, err)
	require.Equal(t, wantPayload, gotPayload)

	// Assert embedded index in CARv2 is same as index generated from the original CARv1.
	wantIdx, err := GenerateIndexFromFile(src)
	require.NoError(t, err)
	ir, err := subject.IndexReader()
	require.NoError(t, err)
	gotIdx, err := index.ReadFrom(ir)
	require.NoError(t, err)
	require.Equal(t, wantIdx, gotIdx)
}

func TestExtractV1(t *testing.T) {
	// Produce a CARv1 file to test.
	dagSvc := dstest.Mock()
	v1Src := filepath.Join(t.TempDir(), "original-test-v1.car")
	v1f, err := os.Create(v1Src)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, v1f.Close()) })
	require.NoError(t, carv1.WriteCar(context.Background(), dagSvc, generateRootCid(t, dagSvc), v1f))
	_, err = v1f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	wantV1, err := io.ReadAll(v1f)
	require.NoError(t, err)

	// Wrap the produced CARv1 into a CARv2 to use for testing.
	v2path := filepath.Join(t.TempDir(), "wrapped-for-extract-test-v2.car")
	require.NoError(t, WrapV1File(v1Src, v2path))

	// Assert extract from CARv2 file is as expected.
	dstPath := filepath.Join(t.TempDir(), "extract-file-test-v1.car")
	require.NoError(t, ExtractV1File(v2path, dstPath))
	gotFromFile, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	require.Equal(t, wantV1, gotFromFile)

	// Assert extract from CARv2 file in-place is as expected
	require.NoError(t, ExtractV1File(v2path, v2path))
	gotFromInPlaceFile, err := os.ReadFile(v2path)
	require.NoError(t, err)
	require.Equal(t, wantV1, gotFromInPlaceFile)
}

func TestExtractV1WithUnknownVersionIsError(t *testing.T) {
	dstPath := filepath.Join(t.TempDir(), "extract-dst-file-test-v42.car")
	err := ExtractV1File("testdata/sample-rootless-v42.car", dstPath)
	require.EqualError(t, err, "source version must be 2; got: 42")
}

func TestExtractV1FromACarV1IsError(t *testing.T) {
	dstPath := filepath.Join(t.TempDir(), "extract-dst-file-test-v1.car")
	err := ExtractV1File("testdata/sample-v1.car", dstPath)
	require.Equal(t, ErrAlreadyV1, err)
}

func generateRootCid(t *testing.T, adder format.NodeAdder) []cid.Cid {
	// TODO convert this into a utility testing lib that takes an rng and generates a random DAG with some threshold for depth/breadth.
	this := merkledag.NewRawNode([]byte("fish"))
	that := merkledag.NewRawNode([]byte("lobster"))
	other := merkledag.NewRawNode([]byte("🌊"))

	one := &merkledag.ProtoNode{}
	assertAddNodeLink(t, one, this, "fishmonger")

	another := &merkledag.ProtoNode{}
	assertAddNodeLink(t, another, one, "barreleye")
	assertAddNodeLink(t, another, that, "🐡")

	andAnother := &merkledag.ProtoNode{}
	assertAddNodeLink(t, andAnother, another, "🍤")

	assertAddNodes(t, adder, this, that, other, one, another, andAnother)
	return []cid.Cid{andAnother.Cid()}
}

func assertAddNodeLink(t *testing.T, pn *merkledag.ProtoNode, fn format.Node, name string) {
	assert.NoError(t, pn.AddNodeLink(name, fn))
}

func assertAddNodes(t *testing.T, adder format.NodeAdder, nds ...format.Node) {
	for _, nd := range nds {
		assert.NoError(t, adder.Add(context.Background(), nd))
	}
}

func TestReplaceRootsInFile(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		roots      []cid.Cid
		wantErrMsg string
	}{
		{
			name:       "CorruptPragmaIsRejected",
			path:       "testdata/sample-corrupt-pragma.car",
			wantErrMsg: "unexpected EOF",
		},
		{
			name:       "CARv42IsRejected",
			path:       "testdata/sample-rootless-v42.car",
			wantErrMsg: "invalid car version: 42",
		},
		{
			name:       "CARv1RootsOfDifferentSizeAreNotReplaced",
			path:       "testdata/sample-v1.car",
			wantErrMsg: "current header size (61) must match replacement header size (18)",
		},
		{
			name:       "CARv2RootsOfDifferentSizeAreNotReplaced",
			path:       "testdata/sample-wrapped-v2.car",
			wantErrMsg: "current header size (61) must match replacement header size (18)",
		},
		{
			name:       "CARv1NonEmptyRootsOfDifferentSizeAreNotReplaced",
			path:       "testdata/sample-v1.car",
			roots:      []cid.Cid{requireDecodedCid(t, "QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n")},
			wantErrMsg: "current header size (61) must match replacement header size (57)",
		},
		{
			name:       "CARv1ZeroLenNonEmptyRootsOfDifferentSizeAreNotReplaced",
			path:       "testdata/sample-v1-with-zero-len-section.car",
			roots:      []cid.Cid{merkledag.NewRawNode([]byte("fish")).Cid()},
			wantErrMsg: "current header size (61) must match replacement header size (59)",
		},
		{
			name:       "CARv2NonEmptyRootsOfDifferentSizeAreNotReplaced",
			path:       "testdata/sample-wrapped-v2.car",
			roots:      []cid.Cid{merkledag.NewRawNode([]byte("fish")).Cid()},
			wantErrMsg: "current header size (61) must match replacement header size (59)",
		},
		{
			name:       "CARv2IndexlessNonEmptyRootsOfDifferentSizeAreNotReplaced",
			path:       "testdata/sample-v2-indexless.car",
			roots:      []cid.Cid{merkledag.NewRawNode([]byte("fish")).Cid()},
			wantErrMsg: "current header size (61) must match replacement header size (59)",
		},
		{
			name:  "CARv1SameSizeRootsAreReplaced",
			path:  "testdata/sample-v1.car",
			roots: []cid.Cid{requireDecodedCid(t, "bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5od")},
		},
		{
			name:  "CARv2SameSizeRootsAreReplaced",
			path:  "testdata/sample-wrapped-v2.car",
			roots: []cid.Cid{requireDecodedCid(t, "bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oi")},
		},
		{
			name:  "CARv2IndexlessSameSizeRootsAreReplaced",
			path:  "testdata/sample-v2-indexless.car",
			roots: []cid.Cid{requireDecodedCid(t, "bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5oi")},
		},
		{
			name:  "CARv1ZeroLenSameSizeRootsAreReplaced",
			path:  "testdata/sample-v1-with-zero-len-section.car",
			roots: []cid.Cid{requireDecodedCid(t, "bafy2bzaced4ueelaegfs5fqu4tzsh6ywbbpfk3cxppupmxfdhbpbhzawfw5o5")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of input files to preserve original for comparison.
			// This also avoids modification files in testdata.
			tmpCopy := requireTmpCopy(t, tt.path)
			err := ReplaceRootsInFile(tmpCopy, tt.roots)
			if tt.wantErrMsg != "" {
				require.EqualError(t, err, tt.wantErrMsg)
				return
			}
			require.NoError(t, err)

			original, err := os.Open(tt.path)
			require.NoError(t, err)
			defer func() { require.NoError(t, original.Close()) }()

			target, err := os.Open(tmpCopy)
			require.NoError(t, err)
			defer func() { require.NoError(t, target.Close()) }()

			// Assert file size has not changed.
			wantStat, err := original.Stat()
			require.NoError(t, err)
			gotStat, err := target.Stat()
			require.NoError(t, err)
			require.Equal(t, wantStat.Size(), gotStat.Size())

			wantReader, err := NewBlockReader(original, ZeroLengthSectionAsEOF(true))
			require.NoError(t, err)
			gotReader, err := NewBlockReader(target, ZeroLengthSectionAsEOF(true))
			require.NoError(t, err)

			// Assert roots are replaced.
			require.Equal(t, tt.roots, gotReader.Roots)

			// Assert data blocks are identical.
			for {
				wantNext, wantErr := wantReader.Next()
				gotNext, gotErr := gotReader.Next()
				if wantErr == io.EOF {
					require.Equal(t, io.EOF, gotErr)
					break
				}
				require.NoError(t, wantErr)
				require.NoError(t, gotErr)
				require.Equal(t, wantNext, gotNext)
			}
		})
	}
}

func requireDecodedCid(t *testing.T, s string) cid.Cid {
	decoded, err := cid.Decode(s)
	require.NoError(t, err)
	return decoded
}

func requireTmpCopy(t *testing.T, src string) string {
	srcF, err := os.Open(src)
	require.NoError(t, err)
	defer func() { require.NoError(t, srcF.Close()) }()
	stats, err := srcF.Stat()
	require.NoError(t, err)

	dst := filepath.Join(t.TempDir(), stats.Name())
	dstF, err := os.Create(dst)
	require.NoError(t, err)
	defer func() { require.NoError(t, dstF.Close()) }()

	_, err = io.Copy(dstF, srcF)
	require.NoError(t, err)
	return dst
}
