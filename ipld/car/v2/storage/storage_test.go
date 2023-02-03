package storage_test

import (
	"context"
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/ipld/go-car/v2/storage"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var rng = rand.New(rand.NewSource(1413))

func TestReadable(t *testing.T) {
	tests := []struct {
		name       string
		v1OrV2path string
		opts       []carv2.Option
	}{
		{
			"OpenedWithCarV1",
			"../testdata/sample-v1.car",
			[]carv2.Option{blockstore.UseWholeCIDs(true), carv2.StoreIdentityCIDs(true)},
		},
		{
			"OpenedWithCarV1_NoIdentityCID",
			"../testdata/sample-v1.car",
			[]carv2.Option{blockstore.UseWholeCIDs(true)},
		},
		{
			"OpenedWithCarV2",
			"../testdata/sample-wrapped-v2.car",
			[]carv2.Option{blockstore.UseWholeCIDs(true), carv2.StoreIdentityCIDs(true)},
		},
		{
			"OpenedWithCarV2_NoIdentityCID",
			"../testdata/sample-wrapped-v2.car",
			[]carv2.Option{blockstore.UseWholeCIDs(true)},
		},
		{
			"OpenedWithCarV1ZeroLenSection",
			"../testdata/sample-v1-with-zero-len-section.car",
			[]carv2.Option{blockstore.UseWholeCIDs(true), carv2.ZeroLengthSectionAsEOF(true)},
		},
		{
			"OpenedWithAnotherCarV1ZeroLenSection",
			"../testdata/sample-v1-with-zero-len-section2.car",
			[]carv2.Option{blockstore.UseWholeCIDs(true), carv2.ZeroLengthSectionAsEOF(true)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			subjectReader, err := os.Open(tt.v1OrV2path)
			require.NoError(t, err)
			subject, err := storage.NewReadable(subjectReader, tt.opts...)
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
				require.True(t, has)

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

			// not exists
			c := randCid()
			has, err := subject.Has(ctx, c.KeyString())
			require.NoError(t, err)
			require.False(t, has)

			_, err = subject.Get(ctx, c.KeyString())
			require.True(t, errors.Is(err, storage.ErrNotFound{}))
			require.True(t, storage.IsNotFound(err))
			require.Contains(t, err.Error(), c.String())

			// random identity, should only find this if we _don't_ store identity CIDs
			storeIdentity := carv2.ApplyOptions(tt.opts...).StoreIdentityCIDs
			c = randIdentityCid()

			has, err = subject.Has(ctx, c.KeyString())
			require.NoError(t, err)
			require.Equal(t, !storeIdentity, has)

			got, err := subject.Get(ctx, c.KeyString())
			if !storeIdentity {
				require.NoError(t, err)
				mh, err := multihash.Decode(c.Hash())
				require.NoError(t, err)
				require.Equal(t, mh.Digest, got)
			} else {
				require.True(t, errors.Is(err, storage.ErrNotFound{}))
				require.True(t, storage.IsNotFound(err))
				require.Contains(t, err.Error(), c.String())
			}
		})
	}
}

func TestWritable(t *testing.T) {
	originalCarV1Path := "../testdata/sample-v1.car"

	variants := []struct {
		name                  string
		compareCarV1          string
		options               []carv2.Option
		expectedV1StartOffset int64
	}{
		// no options, expect a standard CARv2 with the noidentity inner CARv1
		{"carv2_noopt", "sample-v1-noidentity.car", []carv2.Option{}, int64(carv2.PragmaSize + carv2.HeaderSize)},
		// no options, expect a standard CARv2 with the noidentity inner CARv1
		{"carv2_identity", "sample-v1.car", []carv2.Option{carv2.StoreIdentityCIDs(true)}, int64(carv2.PragmaSize + carv2.HeaderSize)},
		// option to only write as a CARv1, expect the noidentity inner CARv1
		{"carv1", "sample-v1-noidentity.car", []carv2.Option{blockstore.WriteAsCarV1(true)}, int64(0)},
		// option to only write as a CARv1, expect the noidentity inner CARv1
		{"carv1_identity", "sample-v1.car", []carv2.Option{blockstore.WriteAsCarV1(true), carv2.StoreIdentityCIDs(true)}, int64(0)},
	}

	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			opts := carv2.ApplyOptions(variant.options...)

			srcFile, err := os.Open(originalCarV1Path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, srcFile.Close()) })
			r, err := carv1.NewCarReader(srcFile)
			require.NoError(t, err)

			path := filepath.Join("/tmp/", fmt.Sprintf("writable_%s.car", variant.name))
			dstFile, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
			require.NoError(t, err)
			var writer io.Writer = &writerOnly{dstFile}
			if !opts.WriteAsCarV1 {
				writer = &writerAtOnly{dstFile}
			}
			ingester, err := storage.NewWritable(writer, r.Header.Roots, variant.options...)
			require.NoError(t, err)
			t.Cleanup(func() { dstFile.Close() })

			cids := make([]cid.Cid, 0)
			var idCidCount int
			for {
				b, err := r.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)

				err = ingester.Put(ctx, b.Cid().KeyString(), b.RawData())
				require.NoError(t, err)
				cids = append(cids, b.Cid())

				dmh, err := multihash.Decode(b.Cid().Hash())
				require.NoError(t, err)
				if dmh.Code == multihash.IDENTITY {
					idCidCount++
				}

				// try reading a random one:
				candIndex := rng.Intn(len(cids))
				var candidate cid.Cid
				for _, c := range cids {
					if candIndex == 0 {
						candidate = c
						break
					}
					candIndex--
				}
				has, err := ingester.Has(ctx, candidate.KeyString())
				require.NoError(t, err)
				require.True(t, has)

				// not exists
				has, err = ingester.Has(ctx, randCid().KeyString())
				require.NoError(t, err)
				require.False(t, has)

				// random identity
				has, err = ingester.Has(ctx, randIdentityCid().KeyString())
				require.NoError(t, err)
				require.Equal(t, !opts.StoreIdentityCIDs, has)
			}

			err = ingester.Finalize()
			require.NoError(t, err)

			err = dstFile.Close()
			require.NoError(t, err)

			reopen, err := os.Open(path)
			require.NoError(t, err)
			rd, err := carv2.NewReader(reopen)
			require.NoError(t, err)
			require.Equal(t, opts.WriteAsCarV1, rd.Version == 1)
			require.NoError(t, reopen.Close())

			robs, err := blockstore.OpenReadOnly(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, robs.Close()) })

			allKeysCh, err := robs.AllKeysChan(ctx)
			require.NoError(t, err)
			numKeysCh := 0
			for c := range allKeysCh {
				b, err := robs.Get(ctx, c)
				require.NoError(t, err)
				if !b.Cid().Equals(c) {
					t.Fatal("wrong item returned")
				}
				numKeysCh++
			}
			expectedCidCount := len(cids)
			if !opts.StoreIdentityCIDs {
				expectedCidCount -= idCidCount
			}
			require.Equal(t, expectedCidCount, numKeysCh, "AllKeysChan returned an unexpected amount of keys; expected %v but got %v", expectedCidCount, numKeysCh)

			for _, c := range cids {
				b, err := robs.Get(ctx, c)
				require.NoError(t, err)
				if !b.Cid().Equals(c) {
					t.Fatal("wrong item returned")
				}
			}

			comparePath := filepath.Join("../testdata/", variant.compareCarV1)
			compareStat, err := os.Stat(comparePath)
			require.NoError(t, err)

			wrote, err := os.Open(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wrote.Close()) })
			_, err = wrote.Seek(variant.expectedV1StartOffset, io.SeekStart)
			require.NoError(t, err)
			hasher := sha512.New()
			gotWritten, err := io.Copy(hasher, io.LimitReader(wrote, compareStat.Size()))
			require.NoError(t, err)
			gotSum := hasher.Sum(nil)

			hasher.Reset()
			compareV1, err := os.Open(comparePath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, compareV1.Close()) })
			wantWritten, err := io.Copy(hasher, compareV1)
			require.NoError(t, err)
			wantSum := hasher.Sum(nil)

			require.Equal(t, wantWritten, gotWritten)
			require.Equal(t, wantSum, gotSum)
		})
	}
}

type writerOnly struct {
	io.Writer
}

func (w *writerOnly) Write(p []byte) (n int, err error) {
	return w.Writer.Write(p)
}

type writerAtOnly struct {
	*os.File
}

func (w *writerAtOnly) WriteAt(p []byte, off int64) (n int, err error) {
	return w.File.WriteAt(p, off)
}

func (w *writerAtOnly) Write(p []byte) (n int, err error) {
	return w.File.Write(p)
}

func randCid() cid.Cid {
	b := make([]byte, 32)
	rng.Read(b)
	mh, _ := multihash.Encode(b, multihash.SHA2_256)
	return cid.NewCidV1(cid.DagProtobuf, mh)
}

func randIdentityCid() cid.Cid {
	b := make([]byte, 32)
	rng.Read(b)
	mh, _ := multihash.Encode(b, multihash.IDENTITY)
	return cid.NewCidV1(cid.Raw, mh)
}
