package blockstore

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-merkledag"
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
			subject, err := OpenReadOnly(tt.v1OrV2path, tt.opts...)
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

				// Assert write operations error
				require.Error(t, subject.Put(wantBlock))
				require.Error(t, subject.PutMany([]blocks.Block{wantBlock}))
				require.Error(t, subject.DeleteBlock(key))
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

func TestReadOnlyAllKeysChanErrHandlerCalledOnTimeout(t *testing.T) {
	expiredCtx, cancel := context.WithTimeout(context.Background(), -time.Millisecond)
	t.Cleanup(cancel)

	subject, err := OpenReadOnly("../testdata/sample-v1.car")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, subject.Close()) })

	// Make a channel to be able to select and block on until error handler is called.
	errHandlerCalled := make(chan interface{})
	expiredErrHandlingCtx := WithAsyncErrorHandler(expiredCtx, func(err error) {
		defer close(errHandlerCalled)
		require.EqualError(t, err, "context deadline exceeded")
	})
	_, err = subject.AllKeysChan(expiredErrHandlingCtx)
	require.NoError(t, err)

	// Assert error handler was called with required condition, waiting at most 3 seconds.
	select {
	case <-errHandlerCalled:
		break
	case <-time.After(time.Second * 3):
		require.Fail(t, "error handler was not called within expected time window")
	}
}

func TestReadOnlyAllKeysChanErrHandlerNeverCalled(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		errHandler func(err error)
		wantCIDs   []cid.Cid
	}{
		{
			"ReadingValidCarV1ReturnsNoErrors",
			"../testdata/sample-v1.car",
			func(err error) {
				require.Fail(t, "unexpected call", "error handler called unexpectedly with err: %v", err)
			},
			listCids(t, newV1ReaderFromV1File(t, "../testdata/sample-v1.car", false)),
		},
		{
			"ReadingValidCarV2ReturnsNoErrors",
			"../testdata/sample-wrapped-v2.car",
			func(err error) {
				require.Fail(t, "unexpected call", "error handler called unexpectedly with err: %v", err)
			},
			listCids(t, newV1ReaderFromV2File(t, "../testdata/sample-wrapped-v2.car", false)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject, err := OpenReadOnly(tt.path, UseWholeCIDs(true))
			require.NoError(t, err)
			ctx := WithAsyncErrorHandler(context.Background(), tt.errHandler)
			keysChan, err := subject.AllKeysChan(ctx)
			require.NoError(t, err)
			var gotCids []cid.Cid
			for k := range keysChan {
				gotCids = append(gotCids, k)
			}
			require.Equal(t, tt.wantCIDs, gotCids)
		})
	}
}

func listCids(t *testing.T, v1r *carv1.CarReader) (cids []cid.Cid) {
	for {
		block, err := v1r.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		cids = append(cids, block.Cid())
	}
	return
}

func newV1ReaderFromV1File(t *testing.T, carv1Path string, zeroLenSectionAsEOF bool) *carv1.CarReader {
	f, err := os.Open(carv1Path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	v1r, err := newV1Reader(f, zeroLenSectionAsEOF)
	require.NoError(t, err)
	return v1r
}

func newV1ReaderFromV2File(t *testing.T, carv2Path string, zeroLenSectionAsEOF bool) *carv1.CarReader {
	f, err := os.Open(carv2Path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	v2r, err := carv2.NewReader(f)
	require.NoError(t, err)
	v1r, err := newV1Reader(v2r.DataReader(), zeroLenSectionAsEOF)
	require.NoError(t, err)
	return v1r
}

func newV1Reader(r io.Reader, zeroLenSectionAsEOF bool) (*carv1.CarReader, error) {
	if zeroLenSectionAsEOF {
		return carv1.NewCarReaderWithZeroLengthSectionAsEOF(r)
	}
	return carv1.NewCarReader(r)
}

func TestReadOnlyErrorAfterClose(t *testing.T) {
	bs, err := OpenReadOnly("../testdata/sample-v1.car")
	require.NoError(t, err)

	roots, err := bs.Roots()
	require.NoError(t, err)
	_, err = bs.Has(roots[0])
	require.NoError(t, err)
	_, err = bs.Get(roots[0])
	require.NoError(t, err)
	_, err = bs.GetSize(roots[0])
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	_, err = bs.AllKeysChan(ctx)
	require.NoError(t, err)
	cancel() // to stop the AllKeysChan goroutine

	bs.Close()

	_, err = bs.Roots()
	require.Error(t, err)
	_, err = bs.Has(roots[0])
	require.Error(t, err)
	_, err = bs.Get(roots[0])
	require.Error(t, err)
	_, err = bs.GetSize(roots[0])
	require.Error(t, err)
	_, err = bs.AllKeysChan(ctx)
	require.Error(t, err)

	// TODO: test that closing blocks if an AllKeysChan operation is
	// in progress.
}
