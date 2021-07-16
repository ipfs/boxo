package car

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	dstest "github.com/ipfs/go-merkledag/test"
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
	wantPayload, err := ioutil.ReadAll(sf)
	require.NoError(t, err)
	gotPayload, err := ioutil.ReadAll(subject.CarV1Reader())
	require.NoError(t, err)
	require.Equal(t, wantPayload, gotPayload)

	// Assert embedded index in CARv2 is same as index generated from the original CARv1.
	wantIdx, err := GenerateIndexFromFile(src)
	require.NoError(t, err)
	gotIdx, err := index.ReadFrom(subject.IndexReader())
	require.NoError(t, err)
	require.Equal(t, wantIdx, gotIdx)
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
