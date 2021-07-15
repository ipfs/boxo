package blockstore

import (
	"context"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-merkledag"
	"io"
	"os"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"

	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyGetReturnsBlockstoreNotFoundWhenCidDoesNotExist(t *testing.T) {
	subject, err := OpenReadOnly("../testdata/sample-v1.car")
	require.NoError(t, err)
	nonExistingKey := merkledag.NewRawNode([]byte("lobstermuncher")).Block.Cid()

	// Assert blockstore API returns blockstore.ErrNotFound
	gotBlock, err := subject.Get(nonExistingKey)
	require.Equal(t, blockstore.ErrNotFound, err)
	require.Nil(t, gotBlock)
}

func TestReadOnly(t *testing.T) {
	tests := []struct {
		name       string
		v1OrV2path string
		v1r        *carv1.CarReader
	}{
		{
			"OpenedWithCarV1",
			"../testdata/sample-v1.car",
			newReaderFromV1File(t, "../testdata/sample-v1.car"),
		},
		{
			"OpenedWithCarV2",
			"../testdata/sample-wrapped-v2.car",
			newReaderFromV2File(t, "../testdata/sample-wrapped-v2.car"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject, err := OpenReadOnly(tt.v1OrV2path)
			t.Cleanup(func() { subject.Close() })
			require.NoError(t, err)

			// Assert roots match v1 payload.
			wantRoots := tt.v1r.Header.Roots
			gotRoots, err := subject.Roots()
			require.NoError(t, err)
			require.Equal(t, wantRoots, gotRoots)

			var wantCids []cid.Cid
			for {
				wantBlock, err := tt.v1r.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)

				key := wantBlock.Cid()
				wantCids = append(wantCids, key)

				// Assert blockstore contains key.
				has, err := subject.Has(key)
				require.NoError(t, err)
				require.True(t, has)

				// Assert size matches block raw data length.
				gotSize, err := subject.GetSize(key)
				wantSize := len(wantBlock.RawData())
				require.NoError(t, err)
				require.Equal(t, wantSize, gotSize)

				// Assert block itself matches v1 payload block.
				gotBlock, err := subject.Get(key)
				require.NoError(t, err)
				require.Equal(t, wantBlock, gotBlock)

				// Assert write operations panic
				require.Panics(t, func() { subject.Put(wantBlock) })
				require.Panics(t, func() { subject.PutMany([]blocks.Block{wantBlock}) })
				require.Panics(t, func() { subject.DeleteBlock(key) })
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			defer cancel()

			// Assert all cids in blockstore match v1 payload CIDs.
			allKeysChan, err := subject.AllKeysChan(ctx)
			require.NoError(t, err)
			var gotCids []cid.Cid
			for gotKey := range allKeysChan {
				gotCids = append(gotCids, gotKey)
			}
			require.Equal(t, wantCids, gotCids)
		})
	}
}

func TestNewReadOnlyFailsOnUnknownVersion(t *testing.T) {
	f, err := os.Open("../testdata/sample-rootless-v42.car")
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	subject, err := NewReadOnly(f, nil)
	require.Errorf(t, err, "unsupported car version: 42")
	require.Nil(t, subject)
}

func newReaderFromV1File(t *testing.T, carv1Path string) *carv1.CarReader {
	f, err := os.Open(carv1Path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	v1r, err := carv1.NewCarReader(f)
	require.NoError(t, err)
	return v1r
}

func newReaderFromV2File(t *testing.T, carv2Path string) *carv1.CarReader {
	f, err := os.Open(carv2Path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	v2r, err := carv2.NewReader(f)
	require.NoError(t, err)
	v1r, err := carv1.NewCarReader(v2r.CarV1Reader())
	require.NoError(t, err)
	return v1r
}
