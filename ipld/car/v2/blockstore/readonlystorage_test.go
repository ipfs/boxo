package blockstore

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-merkledag"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyStorageGetReturnsBlockstoreNotFoundWhenCidDoesNotExist(t *testing.T) {
	subject, err := OpenReadOnlyStorage("../testdata/sample-v1.car")
	require.NoError(t, err)
	nonExistingKey := merkledag.NewRawNode([]byte("lobstermuncher")).Block.Cid()

	// Assert blockstore API returns blockstore.ErrNotFound
	gotBlock, err := subject.Get(context.TODO(), string(nonExistingKey.Bytes()))
	require.Equal(t, blockstore.ErrNotFound, err)
	require.Nil(t, gotBlock)
}

func TestReadOnlyStorage(t *testing.T) {
	tests := []struct {
		name       string
		v1OrV2path string
		opts       []carv2.Option
	}{
		{
			"OpenedWithCarV1",
			"../testdata/sample-v1.car",
			[]carv2.Option{UseWholeCIDs(true), carv2.StoreIdentityCIDs(true)},
		},
		{
			"OpenedWithCarV2",
			"../testdata/sample-wrapped-v2.car",
			[]carv2.Option{UseWholeCIDs(true), carv2.StoreIdentityCIDs(true)},
		},
		{
			"OpenedWithCarV1ZeroLenSection",
			"../testdata/sample-v1-with-zero-len-section.car",
			[]carv2.Option{UseWholeCIDs(true), carv2.ZeroLengthSectionAsEOF(true)},
		},
		{
			"OpenedWithAnotherCarV1ZeroLenSection",
			"../testdata/sample-v1-with-zero-len-section2.car",
			[]carv2.Option{UseWholeCIDs(true), carv2.ZeroLengthSectionAsEOF(true)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			subject, err := OpenReadOnlyStorage(tt.v1OrV2path, tt.opts...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, subject.Close()) })

			f, err := os.Open(tt.v1OrV2path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, f.Close()) })

			reader, err := carv2.NewBlockReader(f, tt.opts...)
			require.NoError(t, err)

			// Assert roots match v1 payload.
			wantRoots := reader.Roots
			gotRoots, err := subject.Roots()
			require.NoError(t, err)
			require.Equal(t, wantRoots, gotRoots)

			var wantCids []cid.Cid
			for {
				wantBlock, err := reader.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)

				key := wantBlock.Cid()
				wantCids = append(wantCids, key)

				// Assert blockstore contains key.
				has, err := subject.Has(ctx, key.KeyString())
				require.NoError(t, err)
				require.True(t, has)

				// Assert block itself matches v1 payload block.
				gotBlock, err := subject.Get(ctx, key.KeyString())
				require.NoError(t, err)
				require.Equal(t, wantBlock.RawData(), gotBlock)

				reader, err := subject.GetStream(ctx, key.KeyString())
				require.NoError(t, err)
				data, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.Equal(t, wantBlock.RawData(), data)
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			defer cancel()
		})
	}
}

func TestNewReadOnlyStorageFailsOnUnknownVersion(t *testing.T) {
	f, err := os.Open("../testdata/sample-rootless-v42.car")
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	subject, err := NewReadOnlyStorage(f, nil)
	require.Errorf(t, err, "unsupported car version: 42")
	require.Nil(t, subject)
}
