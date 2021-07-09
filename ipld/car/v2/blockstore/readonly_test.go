package blockstore

import (
	"io"
	"os"
	"testing"

	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyGetSize(t *testing.T) {
	carv1Path := "testdata/test.car"
	subject, err := OpenReadOnly(carv1Path)
	t.Cleanup(func() { subject.Close() })
	require.NoError(t, err)

	f, err := os.Open(carv1Path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	v1r, err := carv1.NewCarReader(f)
	require.NoError(t, err)
	for {
		wantBlock, err := v1r.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		key := wantBlock.Cid()

		// Assert returned size matches the block.RawData length.
		getSize, err := subject.GetSize(key)
		wantSize := len(wantBlock.RawData())
		require.NoError(t, err)
		require.Equal(t, wantSize, getSize)

		// While at it test blocks are as expected.
		gotBlock, err := subject.Get(key)
		require.NoError(t, err)
		require.Equal(t, wantBlock, gotBlock)
	}
}
