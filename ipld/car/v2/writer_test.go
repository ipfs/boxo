package car

import (
	"bytes"
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	dstest "github.com/ipfs/go-merkledag/test"
	"github.com/ipld/go-car/v2/index"
	"github.com/stretchr/testify/assert"
)

func TestPadding_WriteTo(t *testing.T) {
	tests := []struct {
		name      string
		padding   padding
		wantBytes []byte
		wantN     int64
		wantErr   bool
	}{
		{
			"ZeroPaddingIsNoBytes",
			padding(0),
			nil,
			0,
			false,
		},
		{
			"NonZeroPaddingIsCorrespondingZeroValueBytes",
			padding(3),
			[]byte{0x00, 0x00, 0x00},
			3,
			false,
		},
		{
			"PaddingLargerThanTheBulkPaddingSizeIsCorrespondingZeroValueBytes",
			padding(1025),
			make([]byte, 1025),
			1025,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			gotN, gotErr := tt.padding.WriteTo(w)
			if tt.wantErr {
				assert.Error(t, gotErr)
				return
			}
			gotBytes := w.Bytes()
			assert.Equal(t, tt.wantN, gotN)
			assert.Equal(t, tt.wantBytes, gotBytes)
		})
	}
}

func TestNewWriter(t *testing.T) {
	dagService := dstest.Mock()
	wantRoots := generateRootCid(t, dagService)
	writer := NewWriter(context.Background(), dagService, wantRoots)
	assert.Equal(t, index.IndexSorted, writer.IndexCodec)
	assert.Equal(t, wantRoots, writer.roots)
}

func generateRootCid(t *testing.T, adder format.NodeAdder) []cid.Cid {
	// TODO convert this into a utility testing lib that takes an rng and generates a random DAG with some threshold for depth/breadth.
	this := dag.NewRawNode([]byte("fish"))
	that := dag.NewRawNode([]byte("lobster"))
	other := dag.NewRawNode([]byte("🌊"))

	one := &dag.ProtoNode{}
	assertAddNodeLink(t, one, this, "fishmonger")

	another := &dag.ProtoNode{}
	assertAddNodeLink(t, another, one, "barreleye")
	assertAddNodeLink(t, another, that, "🐡")

	andAnother := &dag.ProtoNode{}
	assertAddNodeLink(t, andAnother, another, "🍤")

	assertAddNodes(t, adder, this, that, other, one, another, andAnother)
	return []cid.Cid{andAnother.Cid()}
}

func assertAddNodeLink(t *testing.T, pn *dag.ProtoNode, fn format.Node, name string) {
	assert.NoError(t, pn.AddNodeLink(name, fn))
}

func assertAddNodes(t *testing.T, adder format.NodeAdder, nds ...format.Node) {
	for _, nd := range nds {
		assert.NoError(t, adder.Add(context.Background(), nd))
	}
}
