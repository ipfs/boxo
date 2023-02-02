package storage_test

import (
	"context"
	"io"
	"os"
	"testing"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/storage"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestReadable(t *testing.T) {
	tests := []struct {
		name       string
		v1OrV2path string
		opts       []carv2.Option
		noIdCids   bool
	}{
		{
			"OpenedWithCarV1",
			"../testdata/sample-v1.car",
			[]carv2.Option{blockstore.UseWholeCIDs(true), carv2.StoreIdentityCIDs(true)},
			// index is made, but identity CIDs are included so they'll be found
			false,
		},
		{
			"OpenedWithCarV1_NoIdentityCID",
			"../testdata/sample-v1.car",
			[]carv2.Option{blockstore.UseWholeCIDs(true)},
			// index is made, identity CIDs are not included, but we always short-circuit when StoreIdentityCIDs(false)
			false,
		},
		{
			"OpenedWithCarV2",
			"../testdata/sample-wrapped-v2.car",
			[]carv2.Option{blockstore.UseWholeCIDs(true), carv2.StoreIdentityCIDs(true)},
			// index already exists, but was made without identity CIDs, but opening with StoreIdentityCIDs(true) means we check the index
			true,
		},
		{
			"OpenedWithCarV2_NoIdentityCID",
			"../testdata/sample-wrapped-v2.car",
			[]carv2.Option{blockstore.UseWholeCIDs(true)},
			// index already exists, it was made without identity CIDs, but we always short-circuit when StoreIdentityCIDs(false)
			false,
		},
		{
			"OpenedWithCarV1ZeroLenSection",
			"../testdata/sample-v1-with-zero-len-section.car",
			[]carv2.Option{blockstore.UseWholeCIDs(true), carv2.ZeroLengthSectionAsEOF(true)},
			false,
		},
		{
			"OpenedWithAnotherCarV1ZeroLenSection",
			"../testdata/sample-v1-with-zero-len-section2.car",
			[]carv2.Option{blockstore.UseWholeCIDs(true), carv2.ZeroLengthSectionAsEOF(true)},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			subjectReader, err := os.Open(tt.v1OrV2path)
			require.NoError(t, err)
			subject, err := storage.NewReadable(subjectReader, nil, tt.opts...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, subjectReader.Close()) })

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

			for {
				wantBlock, err := reader.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)

				key := wantBlock.Cid()

				// Assert blockstore contains key.
				has, err := subject.Has(ctx, key.KeyString())
				require.NoError(t, err)
				if key.Prefix().MhType == uint64(multicodec.Identity) && tt.noIdCids {
					// fixture wasn't made with StoreIdentityCIDs, but we opened it with StoreIdentityCIDs,
					// so they aren't there to find
					require.False(t, has)
				} else {
					require.True(t, has)
				}

				// Assert block itself matches v1 payload block.
				if has {
					gotBlock, err := subject.Get(ctx, key.KeyString())
					require.NoError(t, err)
					require.Equal(t, wantBlock.RawData(), gotBlock)

					reader, err := subject.GetStream(ctx, key.KeyString())
					require.NoError(t, err)
					data, err := io.ReadAll(reader)
					require.NoError(t, err)
					require.Equal(t, wantBlock.RawData(), data)
				}
			}
		})
	}
}
