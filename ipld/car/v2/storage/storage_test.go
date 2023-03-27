package storage_test

// TODO: test readable can't write and writable can't read

import (
	"bytes"
	"context"
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	carv2 "github.com/ipfs/boxo/ipld/car/v2"
	"github.com/ipfs/boxo/ipld/car/v2/index"
	"github.com/ipfs/boxo/ipld/car/v2/internal/carv1"
	"github.com/ipfs/boxo/ipld/car/v2/storage"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var rng = rand.New(rand.NewSource(1413))
var rngLk sync.Mutex

func TestReadable(t *testing.T) {
	tests := []struct {
		name      string
		inputPath string
		opts      []carv2.Option
		noIdCids  bool
	}{
		{
			"OpenedWithCarV1",
			"../testdata/sample-v1.car",
			[]carv2.Option{carv2.UseWholeCIDs(true), carv2.StoreIdentityCIDs(true)},
			false,
		},
		{
			"OpenedWithCarV1_NoIdentityCID",
			"../testdata/sample-v1.car",
			[]carv2.Option{carv2.UseWholeCIDs(true)},
			false,
		},
		{
			"OpenedWithCarV2",
			"../testdata/sample-wrapped-v2.car",
			[]carv2.Option{carv2.UseWholeCIDs(true), carv2.StoreIdentityCIDs(true)},
			// index already exists, but was made without identity CIDs, but opening with StoreIdentityCIDs(true) means we check the index
			true,
		},
		{
			"OpenedWithCarV2_NoIdentityCID",
			"../testdata/sample-wrapped-v2.car",
			[]carv2.Option{carv2.UseWholeCIDs(true)},
			false,
		},
		{
			"OpenedWithCarV1ZeroLenSection",
			"../testdata/sample-v1-with-zero-len-section.car",
			[]carv2.Option{carv2.UseWholeCIDs(true), carv2.ZeroLengthSectionAsEOF(true)},
			false,
		},
		{
			"OpenedWithAnotherCarV1ZeroLenSection",
			"../testdata/sample-v1-with-zero-len-section2.car",
			[]carv2.Option{carv2.UseWholeCIDs(true), carv2.ZeroLengthSectionAsEOF(true)},
			false,
		},
		{
			"IndexlessV2",
			"../testdata/sample-v2-indexless.car",
			[]carv2.Option{carv2.UseWholeCIDs(true)},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			// Setup new StorageCar
			inputReader, err := os.Open(tt.inputPath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, inputReader.Close()) })
			readable, err := storage.OpenReadable(inputReader, tt.opts...)
			require.NoError(t, err)

			// Setup BlockReader to compare against
			actualReader, err := os.Open(tt.inputPath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, actualReader.Close()) })
			actual, err := carv2.NewBlockReader(actualReader, tt.opts...)
			require.NoError(t, err)

			// Assert roots match v1 payload.
			require.Equal(t, actual.Roots, readable.Roots())

			for {
				wantBlock, err := actual.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)

				key := wantBlock.Cid()

				// Assert StorageCar contains key.
				has, err := readable.Has(ctx, key.KeyString())
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
					gotBlock, err := readable.Get(ctx, key.KeyString())
					require.NoError(t, err)
					require.Equal(t, wantBlock.RawData(), gotBlock)

					reader, err := readable.GetStream(ctx, key.KeyString())
					require.NoError(t, err)
					data, err := io.ReadAll(reader)
					require.NoError(t, err)
					require.Equal(t, wantBlock.RawData(), data)
				}
			}

			// test not exists
			c := randCid()
			has, err := readable.Has(ctx, c.KeyString())
			require.NoError(t, err)
			require.False(t, has)

			_, err = readable.Get(ctx, c.KeyString())
			require.True(t, errors.Is(err, storage.ErrNotFound{}))
			require.True(t, storage.IsNotFound(err))
			require.Contains(t, err.Error(), c.String())

			// random identity, should only find this if we _don't_ store identity CIDs
			storeIdentity := carv2.ApplyOptions(tt.opts...).StoreIdentityCIDs
			c = randIdentityCid()

			has, err = readable.Has(ctx, c.KeyString())
			require.NoError(t, err)
			require.Equal(t, !storeIdentity, has)

			got, err := readable.Get(ctx, c.KeyString())
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

func TestReadableBadVersion(t *testing.T) {
	f, err := os.Open("../testdata/sample-rootless-v42.car")
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	subject, err := storage.OpenReadable(f)
	require.Errorf(t, err, "unsupported car version: 42")
	require.Nil(t, subject)
}

func TestWritable(t *testing.T) {
	originalCarV1Path := "../testdata/sample-v1.car"

	variants := []struct {
		name                  string
		compareCarV1          string
		options               []carv2.Option
		expectedV1StartOffset int64
	}{
		{"carv2_noopt", "sample-v1-noidentity.car", []carv2.Option{}, int64(carv2.PragmaSize + carv2.HeaderSize)},
		{"carv2_identity", "sample-v1.car", []carv2.Option{carv2.StoreIdentityCIDs(true)}, int64(carv2.PragmaSize + carv2.HeaderSize)},
		{"carv1", "sample-v1-noidentity.car", []carv2.Option{carv2.WriteAsCarV1(true)}, int64(0)},
		{"carv1_identity", "sample-v1.car", []carv2.Option{carv2.WriteAsCarV1(true), carv2.StoreIdentityCIDs(true)}, int64(0)},
	}

	for _, mode := range []string{"WithRead", "WithoutRead"} {
		t.Run(mode, func(t *testing.T) {
			for _, variant := range variants {
				t.Run(variant.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()

					opts := carv2.ApplyOptions(variant.options...)

					// Setup input file using standard CarV1 reader
					srcFile, err := os.Open(originalCarV1Path)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, srcFile.Close()) })
					r, err := carv1.NewCarReader(srcFile)
					require.NoError(t, err)

					path := filepath.Join(t.TempDir(), fmt.Sprintf("writable_%s_%s.car", mode, variant.name))
					var dstFile *os.File

					var writable *storage.StorageCar
					if mode == "WithoutRead" {
						dstFile, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
						require.NoError(t, err)
						t.Cleanup(func() { dstFile.Close() })
						var writer io.Writer = &writerOnly{dstFile}
						if !opts.WriteAsCarV1 {
							writer = &writerAtOnly{dstFile}
						}
						w, err := storage.NewWritable(writer, r.Header.Roots, variant.options...)
						require.NoError(t, err)
						writable = w.(*storage.StorageCar)
					} else {
						dstFile, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
						require.NoError(t, err)
						t.Cleanup(func() { dstFile.Close() })
						writable, err = storage.NewReadableWritable(dstFile, r.Header.Roots, variant.options...)
						require.NoError(t, err)
					}

					require.Equal(t, r.Header.Roots, writable.Roots())

					cids := make([]cid.Cid, 0)
					var idCidCount int
					for {
						// read from source
						b, err := r.Next()
						if err == io.EOF {
							break
						}
						require.NoError(t, err)

						// write to dest
						err = writable.Put(ctx, b.Cid().KeyString(), b.RawData())
						require.NoError(t, err)
						cids = append(cids, b.Cid())

						dmh, err := multihash.Decode(b.Cid().Hash())
						require.NoError(t, err)
						if dmh.Code == multihash.IDENTITY {
							idCidCount++
						}

						if mode == "WithRead" {
							// writable is a ReadableWritable / StorageCar

							// read back out the one we just wrote
							gotBlock, err := writable.Get(ctx, b.Cid().KeyString())
							require.NoError(t, err)
							require.Equal(t, b.RawData(), gotBlock)

							reader, err := writable.GetStream(ctx, b.Cid().KeyString())
							require.NoError(t, err)
							data, err := io.ReadAll(reader)
							require.NoError(t, err)
							require.Equal(t, b.RawData(), data)

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
							has, err := writable.Has(ctx, candidate.KeyString())
							require.NoError(t, err)
							require.True(t, has)

							// not exists
							c := randCid()
							has, err = writable.Has(ctx, c.KeyString())
							require.NoError(t, err)
							require.False(t, has)
							_, err = writable.Get(ctx, c.KeyString())
							require.True(t, errors.Is(err, storage.ErrNotFound{}))
							require.True(t, storage.IsNotFound(err))
							require.Contains(t, err.Error(), c.String())

							// random identity, should only find this if we _don't_ store identity CIDs
							c = randIdentityCid()
							has, err = writable.Has(ctx, c.KeyString())
							require.NoError(t, err)
							require.Equal(t, !opts.StoreIdentityCIDs, has)

							got, err := writable.Get(ctx, c.KeyString())
							if !opts.StoreIdentityCIDs {
								require.NoError(t, err)
								mh, err := multihash.Decode(c.Hash())
								require.NoError(t, err)
								require.Equal(t, mh.Digest, got)
							} else {
								require.True(t, errors.Is(err, storage.ErrNotFound{}))
								require.True(t, storage.IsNotFound(err))
								require.Contains(t, err.Error(), c.String())
							}
						}
					}

					err = writable.Finalize()
					require.NoError(t, err)

					err = dstFile.Close()
					require.NoError(t, err)

					// test header version using carv2 reader
					reopen, err := os.Open(path)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, reopen.Close()) })
					rd, err := carv2.NewReader(reopen)
					require.NoError(t, err)
					require.Equal(t, opts.WriteAsCarV1, rd.Version == 1)

					// now compare the binary contents of the written file to the expected file
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
		})
	}
}

func TestCannotWriteableV2WithoutWriterAt(t *testing.T) {
	w, err := storage.NewWritable(&writerOnly{os.Stdout}, []cid.Cid{})
	require.Error(t, err)
	require.Nil(t, w)
}

func TestErrorsWhenWritingCidTooLarge(t *testing.T) {
	maxAllowedCidSize := uint64(20)

	path := filepath.Join(t.TempDir(), "writable-with-id-enabled-too-large.car")
	out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, out.Close()) })
	subject, err := storage.NewWritable(out, []cid.Cid{}, carv2.MaxIndexCidSize(maxAllowedCidSize))
	require.NoError(t, err)

	// normal block but shorten the CID to make it acceptable
	testCid, testData := randBlock()
	mh, err := multihash.Decode(testCid.Hash())
	require.NoError(t, err)
	dig := mh.Digest[:10]
	shortMh, err := multihash.Encode(dig, mh.Code)
	require.NoError(t, err)
	testCid = cid.NewCidV1(mh.Code, shortMh)

	err = subject.Put(context.TODO(), testCid.KeyString(), testData)
	require.NoError(t, err)

	// standard CID but too long for options
	testCid, testData = randBlock()
	err = subject.Put(context.TODO(), testCid.KeyString(), testData)
	require.Equal(t, &carv2.ErrCidTooLarge{MaxSize: maxAllowedCidSize, CurrentSize: uint64(testCid.ByteLen())}, err)
}

func TestConcurrentUse(t *testing.T) {
	dst, err := os.OpenFile(filepath.Join(t.TempDir(), "readwrite.car"), os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dst.Close()) })
	wbs, err := storage.NewReadableWritable(dst, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, err)
	t.Cleanup(func() { wbs.Finalize() })

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			testCid, testData := randBlock()

			has, err := wbs.Has(ctx, testCid.KeyString())
			require.NoError(t, err)
			require.False(t, has)

			err = wbs.Put(ctx, testCid.KeyString(), testData)
			require.NoError(t, err)

			got, err := wbs.Get(ctx, testCid.KeyString())
			require.NoError(t, err)
			require.Equal(t, testData, got)
		}()
	}
	wg.Wait()
}

func TestNullPadding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	paddedV1, err := os.ReadFile("../testdata/sample-v1-with-zero-len-section.car")
	require.NoError(t, err)

	readable, err := storage.OpenReadable(bufferReaderAt(paddedV1), carv2.ZeroLengthSectionAsEOF(true))
	require.NoError(t, err)

	roots := readable.Roots()
	require.Len(t, roots, 1)
	has, err := readable.Has(ctx, roots[0].KeyString())
	require.NoError(t, err)
	require.True(t, has)

	actual, err := carv2.NewBlockReader(bytes.NewReader(paddedV1), carv2.ZeroLengthSectionAsEOF(true))
	require.NoError(t, err)

	for {
		wantBlock, err := actual.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		b, err := readable.Get(ctx, wantBlock.Cid().KeyString())
		require.NoError(t, err)
		require.Equal(t, wantBlock.RawData(), b)
	}
}

func TestPutSameHashes(t *testing.T) {
	tdir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// This writable allows duplicate puts, and identifies by multihash as per the default.
	pathAllowDups := filepath.Join(tdir, "writable-allowdup.car")
	dstAllowDups, err := os.OpenFile(pathAllowDups, os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dstAllowDups.Close()) })
	wbsAllowDups, err := storage.NewReadableWritable(dstAllowDups, nil, carv2.AllowDuplicatePuts(true))
	require.NoError(t, err)

	// This writable deduplicates puts by CID.
	pathByCID := filepath.Join(tdir, "writable-dedup-wholecid.car")
	dstByCID, err := os.OpenFile(pathByCID, os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dstByCID.Close()) })
	wbsByCID, err := storage.NewReadableWritable(dstByCID, nil, carv2.UseWholeCIDs(true))
	require.NoError(t, err)

	// This writable deduplicates puts by multihash
	pathByHash := filepath.Join(tdir, "writable-dedup-byhash.car")
	dstByHash, err := os.OpenFile(pathByHash, os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dstByHash.Close()) })
	wbsByHash, err := storage.NewReadableWritable(dstByHash, nil)
	require.NoError(t, err)

	var blockList []struct {
		cid  cid.Cid
		data []byte
	}

	appendBlock := func(data []byte, version, codec uint64) {
		c, err := cid.Prefix{
			Version:  version,
			Codec:    codec,
			MhType:   multihash.SHA2_256,
			MhLength: -1,
		}.Sum(data)
		require.NoError(t, err)
		blockList = append(blockList, struct {
			cid  cid.Cid
			data []byte
		}{c, data})
	}

	// Two raw blocks, meaning we have two unique multihashes.
	// However, we have multiple CIDs for each multihash.
	// We also have two duplicate CIDs.
	data1 := []byte("foo bar")
	appendBlock(data1, 0, cid.DagProtobuf)
	appendBlock(data1, 1, cid.DagProtobuf)
	appendBlock(data1, 1, cid.DagCBOR)
	appendBlock(data1, 1, cid.DagCBOR) // duplicate CID

	data2 := []byte("foo bar baz")
	appendBlock(data2, 0, cid.DagProtobuf)
	appendBlock(data2, 1, cid.DagProtobuf)
	appendBlock(data2, 1, cid.DagProtobuf) // duplicate CID
	appendBlock(data2, 1, cid.DagCBOR)

	countBlocks := func(path string) int {
		f, err := os.Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, f.Close()) })
		rdr, err := carv2.NewBlockReader(f)
		require.NoError(t, err)

		n := 0
		for {
			_, err := rdr.Next()
			if err == io.EOF {
				break
			}
			n++
		}
		return n
	}

	putBlockList := func(writable *storage.StorageCar) {
		for i, block := range blockList {
			// Has should never error here.
			// The first block should be missing.
			// Others might not, given the duplicate hashes.
			has, err := writable.Has(ctx, block.cid.KeyString())
			require.NoError(t, err)
			if i == 0 {
				require.False(t, has)
			}

			err = writable.Put(ctx, block.cid.KeyString(), block.data)
			require.NoError(t, err)

			// Has and Get need to work right after a Put
			has, err = writable.Has(ctx, block.cid.KeyString())
			require.NoError(t, err)
			require.True(t, has)

			got, err := writable.Get(ctx, block.cid.KeyString())
			require.NoError(t, err)
			require.Equal(t, block.data, got)
		}
	}

	putBlockList(wbsAllowDups)
	err = wbsAllowDups.Finalize()
	require.NoError(t, err)
	require.Equal(t, len(blockList), countBlocks(pathAllowDups))

	// Put the same list of blocks to the CAR that deduplicates by CID.
	// We should end up with two fewer blocks, as two are entire CID duplicates.
	putBlockList(wbsByCID)
	err = wbsByCID.Finalize()
	require.NoError(t, err)
	require.Equal(t, len(blockList)-2, countBlocks(pathByCID))

	// Put the same list of blocks to the CAR that deduplicates by CID.
	// We should end up with just two blocks, as the original set of blocks only
	// has two distinct multihashes.
	putBlockList(wbsByHash)
	err = wbsByHash.Finalize()
	require.NoError(t, err)
	require.Equal(t, 2, countBlocks(pathByHash))
}

func TestReadableCantWrite(t *testing.T) {
	inp, err := os.Open("../testdata/sample-v1.car")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, inp.Close()) })
	readable, err := storage.OpenReadable(inp)
	require.NoError(t, err)
	require.ErrorContains(t, readable.(*storage.StorageCar).Put(context.Background(), randCid().KeyString(), []byte("bar")), "read-only")
	// Finalize() is nonsense for a readable, but it should be safe
	require.NoError(t, readable.(*storage.StorageCar).Finalize())
}

func TestWritableCantRead(t *testing.T) {
	// an io.Writer with no io.WriterAt capabilities
	path := filepath.Join(t.TempDir(), "writable.car")
	out, err := os.Create(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, out.Close()) })

	// This should fail because the writer is not an io.WriterAt
	_, err = storage.NewWritable(&writerOnly{out}, nil)
	require.ErrorContains(t, err, "CARv2")
	require.ErrorContains(t, err, "non-seekable")

	writable, err := storage.NewWritable(&writerOnly{out}, nil, carv2.WriteAsCarV1(true))
	require.NoError(t, err)

	_, err = writable.(*storage.StorageCar).Get(context.Background(), randCid().KeyString())
	require.ErrorContains(t, err, "write-only")

	_, err = writable.(*storage.StorageCar).GetStream(context.Background(), randCid().KeyString())
	require.ErrorContains(t, err, "write-only")

	require.NoError(t, writable.Finalize())
}

func TestReadWriteWithPaddingWorksAsExpected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	testCid1, testData1 := randBlock()
	testCid2, testData2 := randBlock()

	wantRoots := []cid.Cid{testCid1, testCid2}
	path := filepath.Join(t.TempDir(), "readwrite-with-padding.car")
	writer, err := os.Create(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })

	wantCarV1Padding := uint64(1413)
	wantIndexPadding := uint64(1314)
	subject, err := storage.NewReadableWritable(
		writer,
		wantRoots,
		carv2.UseDataPadding(wantCarV1Padding),
		carv2.UseIndexPadding(wantIndexPadding))
	require.NoError(t, err)
	require.NoError(t, subject.Put(ctx, testCid1.KeyString(), testData1))
	require.NoError(t, subject.Put(ctx, testCid2.KeyString(), testData2))
	require.NoError(t, subject.Finalize())

	// Assert CARv2 header contains right offsets.
	gotCarV2, err := carv2.OpenReader(path)
	t.Cleanup(func() { gotCarV2.Close() })
	require.NoError(t, err)
	wantCarV1Offset := carv2.PragmaSize + carv2.HeaderSize + wantCarV1Padding
	wantIndexOffset := wantCarV1Offset + gotCarV2.Header.DataSize + wantIndexPadding
	require.Equal(t, wantCarV1Offset, gotCarV2.Header.DataOffset)
	require.Equal(t, wantIndexOffset, gotCarV2.Header.IndexOffset)
	require.NoError(t, gotCarV2.Close())

	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	// Assert reading CARv1 directly at offset and size is as expected.
	gotCarV1, err := carv1.NewCarReader(io.NewSectionReader(f, int64(wantCarV1Offset), int64(gotCarV2.Header.DataSize)))
	require.NoError(t, err)
	require.Equal(t, wantRoots, gotCarV1.Header.Roots)
	gotBlock, err := gotCarV1.Next()
	require.NoError(t, err)
	require.Equal(t, testCid1, gotBlock.Cid())
	require.Equal(t, testData1, gotBlock.RawData())
	gotBlock, err = gotCarV1.Next()
	require.NoError(t, err)
	require.Equal(t, testCid2, gotBlock.Cid())
	require.Equal(t, testData2, gotBlock.RawData())

	_, err = gotCarV1.Next()
	require.Equal(t, io.EOF, err)

	// Assert reading index directly from file is parsable and has expected CIDs.
	stat, err := f.Stat()
	require.NoError(t, err)
	indexSize := stat.Size() - int64(wantIndexOffset)
	gotIdx, err := index.ReadFrom(io.NewSectionReader(f, int64(wantIndexOffset), indexSize))
	require.NoError(t, err)
	_, err = index.GetFirst(gotIdx, testCid1)
	require.NoError(t, err)
	_, err = index.GetFirst(gotIdx, testCid2)
	require.NoError(t, err)
}

func TestResumption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	srcPath := "../testdata/sample-v1.car"

	v1f, err := os.Open(srcPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, v1f.Close()) })
	rd, err := carv2.NewReader(v1f)
	require.NoError(t, err)
	roots, err := rd.Roots()
	require.NoError(t, err)

	blockSource := func() <-chan simpleBlock {
		v1f, err := os.Open(srcPath)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, v1f.Close()) })
		r, err := carv1.NewCarReader(v1f)
		require.NoError(t, err)
		ret := make(chan simpleBlock)

		go func() {
			for {
				b, err := r.Next()
				if err == io.EOF {
					close(ret)
					break
				}
				require.NoError(t, err)
				ret <- simpleBlock{cid: b.Cid(), data: b.RawData()}
			}
		}()

		return ret
	}

	path := filepath.Join(t.TempDir(), "readwrite-resume.car")
	writer, err := os.Create(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	// Create an incomplete CARv2 file with no blocks put.
	subject, err := storage.NewReadableWritable(writer, roots, carv2.UseWholeCIDs(true))
	require.NoError(t, err)

	// For each block resume on the same file, putting blocks one at a time.
	var wantBlockCountSoFar, idCidCount int
	wantBlocks := make(map[cid.Cid]simpleBlock)
	for b := range blockSource() {
		wantBlockCountSoFar++
		wantBlocks[b.cid] = b

		dmh, err := multihash.Decode(b.cid.Hash())
		require.NoError(t, err)
		if dmh.Code == multihash.IDENTITY {
			idCidCount++
		}

		// 30% chance of subject failing; more concretely: re-instantiating the StorageCar with the same
		// file without calling Finalize. The higher this percentage the slower the test runs
		// considering the number of blocks in the original CARv1 test payload.
		resume := rng.Float32() <= 0.3
		// If testing resume case, then flip a coin to decide whether to finalize before the StorageCar
		// re-instantiation or not. Note, both cases should work for resumption since we do not
		// limit resumption to unfinalized files.
		finalizeBeforeResumption := rng.Float32() <= 0.5
		if resume {
			if finalizeBeforeResumption {
				require.NoError(t, subject.Finalize())
			}

			_, err := writer.Seek(0, io.SeekStart)
			require.NoError(t, err)
			subject, err = storage.OpenReadableWritable(writer, roots, carv2.UseWholeCIDs(true))
			require.NoError(t, err)
		}
		require.NoError(t, subject.Put(ctx, b.cid.KeyString(), b.data))

		// With 10% chance test read operations on an resumed read-write StorageCar.
		// We don't test on every put to reduce test runtime.
		testRead := rng.Float32() <= 0.1
		if testRead {
			// Assert read operations on the read-write StorageCar are as expected when resumed from an
			// existing file
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			t.Cleanup(cancel)
			for k, wantBlock := range wantBlocks {
				has, err := subject.Has(ctx, k.KeyString())
				require.NoError(t, err)
				require.True(t, has)
				gotBlock, err := subject.Get(ctx, k.KeyString())
				require.NoError(t, err)
				require.Equal(t, wantBlock.data, gotBlock)
			}
			// Assert the number of blocks in file are as expected calculated via AllKeysChan
			require.Equal(t, wantBlockCountSoFar, len(wantBlocks))
		}
	}

	// Finalize the StorageCar to complete partially written CARv2 file.
	subject, err = storage.OpenReadableWritable(writer, roots, carv2.UseWholeCIDs(true))
	require.NoError(t, err)
	require.NoError(t, subject.Finalize())

	// Assert resumed from file is a valid CARv2 with index.
	v2f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, v2f.Close()) })
	v2r, err := carv2.NewReader(v2f)
	require.NoError(t, err)
	require.True(t, v2r.Header.HasIndex())

	// Assert CARv1 payload in file matches the original CARv1 payload.
	_, err = v1f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	wantPayloadReader, err := carv1.NewCarReader(v1f)
	require.NoError(t, err)

	dr, err := v2r.DataReader()
	require.NoError(t, err)
	gotPayloadReader, err := carv1.NewCarReader(dr)
	require.NoError(t, err)

	require.Equal(t, wantPayloadReader.Header, gotPayloadReader.Header)
	for {
		wantNextBlock, wantErr := wantPayloadReader.Next()
		if wantErr == io.EOF {
			gotNextBlock, gotErr := gotPayloadReader.Next()
			require.Equal(t, wantErr, gotErr)
			require.Nil(t, gotNextBlock)
			break
		}
		require.NoError(t, wantErr)

		dmh, err := multihash.Decode(wantNextBlock.Cid().Hash())
		require.NoError(t, err)
		if dmh.Code == multihash.IDENTITY {
			continue
		}

		gotNextBlock, gotErr := gotPayloadReader.Next()
		require.NoError(t, gotErr)
		require.Equal(t, wantNextBlock, gotNextBlock)
	}

	// Assert index in resumed from file is identical to index generated from the data payload portion of the generated CARv2 file.
	_, err = v1f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	ir, err := v2r.IndexReader()
	require.NoError(t, err)
	gotIdx, err := index.ReadFrom(ir)
	require.NoError(t, err)
	dr, err = v2r.DataReader()
	require.NoError(t, err)
	wantIdx, err := carv2.GenerateIndex(dr)
	require.NoError(t, err)
	require.Equal(t, wantIdx, gotIdx)
}

func TestResumptionV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	srcPath := "../testdata/sample-v1.car"

	v1f, err := os.Open(srcPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, v1f.Close()) })
	rd, err := carv2.NewReader(v1f)
	require.NoError(t, err)
	roots, err := rd.Roots()
	require.NoError(t, err)

	blockSource := func() <-chan simpleBlock {
		v1f, err := os.Open(srcPath)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, v1f.Close()) })
		r, err := carv1.NewCarReader(v1f)
		require.NoError(t, err)
		ret := make(chan simpleBlock)

		go func() {
			for {
				b, err := r.Next()
				if err == io.EOF {
					close(ret)
					break
				}
				require.NoError(t, err)
				ret <- simpleBlock{cid: b.Cid(), data: b.RawData()}
			}
		}()

		return ret
	}

	path := filepath.Join(t.TempDir(), "readwrite-resume-v1.car")
	writer, err := os.Create(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	// Create an incomplete CARv2 file with no blocks put.
	subject, err := storage.NewReadableWritable(writer, roots, carv2.UseWholeCIDs(true), carv2.WriteAsCarV1(true))
	require.NoError(t, err)

	// For each block resume on the same file, putting blocks one at a time.
	var wantBlockCountSoFar, idCidCount int
	wantBlocks := make(map[cid.Cid]simpleBlock)
	for b := range blockSource() {
		wantBlockCountSoFar++
		wantBlocks[b.cid] = b

		dmh, err := multihash.Decode(b.cid.Hash())
		require.NoError(t, err)
		if dmh.Code == multihash.IDENTITY {
			idCidCount++
		}

		// 30% chance of subject failing; more concretely: re-instantiating the StorageCar with the same
		// file without calling Finalize. The higher this percentage the slower the test runs
		// considering the number of blocks in the original CARv1 test payload.
		resume := rng.Float32() <= 0.3
		// If testing resume case, then flip a coin to decide whether to finalize before the StorageCar
		// re-instantiation or not. Note, both cases should work for resumption since we do not
		// limit resumption to unfinalized files.
		finalizeBeforeResumption := rng.Float32() <= 0.5
		if resume {
			if finalizeBeforeResumption {
				require.NoError(t, subject.Finalize())
			}

			_, err := writer.Seek(0, io.SeekStart)
			require.NoError(t, err)
			subject, err = storage.OpenReadableWritable(writer, roots, carv2.UseWholeCIDs(true), carv2.WriteAsCarV1(true))
			require.NoError(t, err)
		}
		require.NoError(t, subject.Put(ctx, b.cid.KeyString(), b.data))

		// With 10% chance test read operations on an resumed read-write StorageCar.
		// We don't test on every put to reduce test runtime.
		testRead := rng.Float32() <= 0.1
		if testRead {
			// Assert read operations on the read-write StorageCar are as expected when resumed from an
			// existing file
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			t.Cleanup(cancel)
			for k, wantBlock := range wantBlocks {
				has, err := subject.Has(ctx, k.KeyString())
				require.NoError(t, err)
				require.True(t, has)
				gotBlock, err := subject.Get(ctx, k.KeyString())
				require.NoError(t, err)
				require.Equal(t, wantBlock.data, gotBlock)
			}
			// Assert the number of blocks in file are as expected calculated via AllKeysChan
			require.Equal(t, wantBlockCountSoFar, len(wantBlocks))
		}
	}

	// Finalize the StorageCar to complete partially written CARv2 file.
	subject, err = storage.OpenReadableWritable(writer, roots, carv2.UseWholeCIDs(true), carv2.WriteAsCarV1(true))
	require.NoError(t, err)
	require.NoError(t, subject.Finalize())

	// Assert resumed from file is a valid CARv2 with index.
	v2f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, v2f.Close()) })
	v2r, err := carv2.NewReader(v2f)
	require.NoError(t, err)
	require.False(t, v2r.Header.HasIndex())
	require.Equal(t, uint64(1), v2r.Version)

	_, err = v1f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	wantPayloadReader, err := carv1.NewCarReader(v1f)
	require.NoError(t, err)

	dr, err := v2r.DataReader() // since this is a v1 we're just reading from the top with this
	require.NoError(t, err)
	gotPayloadReader, err := carv1.NewCarReader(dr)
	require.NoError(t, err)

	require.Equal(t, wantPayloadReader.Header, gotPayloadReader.Header)
	for {
		wantNextBlock, wantErr := wantPayloadReader.Next()
		if wantErr == io.EOF {
			gotNextBlock, gotErr := gotPayloadReader.Next()
			require.Equal(t, wantErr, gotErr)
			require.Nil(t, gotNextBlock)
			break
		}
		require.NoError(t, wantErr)

		dmh, err := multihash.Decode(wantNextBlock.Cid().Hash())
		require.NoError(t, err)
		if dmh.Code == multihash.IDENTITY {
			continue
		}

		gotNextBlock, gotErr := gotPayloadReader.Next()
		require.NoError(t, gotErr)
		require.Equal(t, wantNextBlock, gotNextBlock)
	}
}

func TestResumptionIsSupportedOnFinalizedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readwrite-resume-finalized.car")
	v2f, err := os.Create(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, v2f.Close()) })
	// Create an incomplete CARv2 file with no blocks put.
	subject, err := storage.NewReadableWritable(v2f, []cid.Cid{})
	require.NoError(t, err)
	require.NoError(t, subject.Finalize())

	reopen, err := os.OpenFile(path, os.O_RDWR, 0o666)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopen.Close()) })
	subject, err = storage.NewReadableWritable(reopen, []cid.Cid{})
	require.NoError(t, err)
	t.Cleanup(func() { subject.Finalize() })
}

func TestReadWriteErrorsOnlyWhenFinalized(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	testCid1, testData1 := randBlock()
	testCid2, testData2 := randBlock()

	wantRoots := []cid.Cid{testCid1, testCid2}
	path := filepath.Join(t.TempDir(), "readwrite-finalized-panic.car")
	writer, err := os.Create(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })

	subject, err := storage.NewReadableWritable(writer, wantRoots)
	require.NoError(t, err)

	require.NoError(t, subject.Put(ctx, testCid1.KeyString(), testData1))
	require.NoError(t, subject.Put(ctx, testCid2.KeyString(), testData2))

	gotBlock, err := subject.Get(ctx, testCid1.KeyString())
	require.NoError(t, err)
	require.Equal(t, testData1, gotBlock)

	gotRoots := subject.Roots()
	require.Equal(t, wantRoots, gotRoots)

	has, err := subject.Has(ctx, testCid1.KeyString())
	require.NoError(t, err)
	require.True(t, has)

	require.NoError(t, subject.Finalize())
	require.Error(t, subject.Finalize())

	_, ok := (interface{})(subject).(io.Closer)
	require.False(t, ok)

	_, err = subject.Get(ctx, testCid1.KeyString())
	require.Error(t, err)
	require.Error(t, err)
	_, err = subject.Has(ctx, testCid2.KeyString())
	require.Error(t, err)

	require.Error(t, subject.Put(ctx, testCid1.KeyString(), testData1))
}

func TestReadWriteResumptionMismatchingRootsIsError(t *testing.T) {
	tmpPath := requireTmpCopy(t, "../testdata/sample-wrapped-v2.car")

	origContent, err := os.ReadFile(tmpPath)
	require.NoError(t, err)

	badRoot := randCid()
	writer, err := os.OpenFile(tmpPath, os.O_RDWR, 0o666)
	require.NoError(t, err)
	t.Cleanup(func() { writer.Close() })
	subject, err := storage.OpenReadableWritable(writer, []cid.Cid{badRoot})
	require.EqualError(t, err, "cannot resume on file with mismatching data header")
	require.Nil(t, subject)
	require.NoError(t, writer.Close())

	newContent, err := os.ReadFile(tmpPath)
	require.NoError(t, err)

	// Expect the bad file to be left untouched; check the size first.
	// If the sizes mismatch, printing a huge diff would not help us.
	require.Equal(t, len(origContent), len(newContent))
	require.Equal(t, origContent, newContent)
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

func TestReadWriteResumptionFromFileWithDifferentCarV1PaddingIsError(t *testing.T) {
	testCid1, testData1 := randBlock()
	wantRoots := []cid.Cid{testCid1}
	path := filepath.Join(t.TempDir(), "readwrite-resume-with-padding.car")
	writer, err := os.Create(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	subject, err := storage.NewReadableWritable(
		writer,
		wantRoots,
		carv2.UseDataPadding(1413))
	require.NoError(t, err)
	require.NoError(t, subject.Put(context.TODO(), testCid1.KeyString(), testData1))
	require.NoError(t, subject.Finalize())

	subject, err = storage.OpenReadableWritable(
		writer,
		wantRoots,
		carv2.UseDataPadding(1314))
	require.EqualError(t, err, "cannot resume from file with mismatched CARv1 offset; "+
		"`WithDataPadding` option must match the padding on file. "+
		"Expected padding value of 1413 but got 1314")
	require.Nil(t, subject)
}

func TestOperationsErrorWithBadCidStrings(t *testing.T) {
	testCid, testData := randBlock()
	path := filepath.Join(t.TempDir(), "badkeys.car")
	writer, err := os.Create(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	subject, err := storage.NewReadableWritable(writer, []cid.Cid{})
	require.NoError(t, err)

	require.NoError(t, subject.Put(context.TODO(), testCid.KeyString(), testData))
	require.ErrorContains(t, subject.Put(context.TODO(), fmt.Sprintf("%s/nope", testCid.KeyString()), testData), "bad CID key")
	require.ErrorContains(t, subject.Put(context.TODO(), "nope", testData), "bad CID key")

	has, err := subject.Has(context.TODO(), testCid.KeyString())
	require.NoError(t, err)
	require.True(t, has)
	has, err = subject.Has(context.TODO(), fmt.Sprintf("%s/nope", testCid.KeyString()))
	require.ErrorContains(t, err, "bad CID key")
	require.False(t, has)
	has, err = subject.Has(context.TODO(), "nope")
	require.ErrorContains(t, err, "bad CID key")
	require.False(t, has)

	got, err := subject.Get(context.TODO(), testCid.KeyString())
	require.NoError(t, err)
	require.NotNil(t, got)
	got, err = subject.Get(context.TODO(), fmt.Sprintf("%s/nope", testCid.KeyString()))
	require.ErrorContains(t, err, "bad CID key")
	require.Nil(t, got)
	got, err = subject.Get(context.TODO(), "nope")
	require.ErrorContains(t, err, "bad CID key")
	require.Nil(t, got)
}

func TestWholeCID(t *testing.T) {
	for _, whole := range []bool{true, false} {
		whole := whole
		t.Run(fmt.Sprintf("whole=%t", whole), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), fmt.Sprintf("writable_%t.car", whole))
			out, err := os.Create(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, out.Close()) })
			store, err := storage.NewReadableWritable(out, []cid.Cid{}, carv2.UseWholeCIDs(whole))
			require.NoError(t, err)

			c1, b1 := randBlock()
			c2, b2 := randBlock()

			require.NoError(t, store.Put(ctx, c1.KeyString(), b1))
			has, err := store.Has(ctx, c1.KeyString())
			require.NoError(t, err)
			require.True(t, has)

			pref := c1.Prefix()
			pref.Codec = cid.DagProtobuf
			pref.Version = 1
			cpb1, err := pref.Sum(b1)
			require.NoError(t, err)

			has, err = store.Has(ctx, cpb1.KeyString())
			require.NoError(t, err)
			require.Equal(t, has, !whole)

			require.NoError(t, store.Put(ctx, c2.KeyString(), b1))
			has, err = store.Has(ctx, c2.KeyString())
			require.NoError(t, err)
			require.True(t, has)
			has, err = store.Has(ctx, cpb1.KeyString())
			require.NoError(t, err)
			require.Equal(t, has, !whole)

			pref = c2.Prefix()
			pref.Codec = cid.DagProtobuf
			pref.Version = 1
			cpb2, err := pref.Sum(b2)
			require.NoError(t, err)

			has, err = store.Has(ctx, cpb2.KeyString())
			require.NoError(t, err)
			require.Equal(t, has, !whole)
			has, err = store.Has(ctx, cpb1.KeyString())
			require.NoError(t, err)
			require.Equal(t, has, !whole)
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

func randBlock() (cid.Cid, []byte) {
	data := make([]byte, 1024)
	rngLk.Lock()
	rng.Read(data)
	rngLk.Unlock()
	h, err := multihash.Sum(data, multihash.SHA2_512, -1)
	if err != nil {
		panic(err)
	}
	return cid.NewCidV1(cid.Raw, h), data
}

func randCid() cid.Cid {
	b := make([]byte, 32)
	rngLk.Lock()
	rng.Read(b)
	rngLk.Unlock()
	mh, _ := multihash.Encode(b, multihash.SHA2_256)
	return cid.NewCidV1(cid.DagProtobuf, mh)
}

func randIdentityCid() cid.Cid {
	b := make([]byte, 32)
	rngLk.Lock()
	rng.Read(b)
	rngLk.Unlock()
	mh, _ := multihash.Encode(b, multihash.IDENTITY)
	return cid.NewCidV1(cid.Raw, mh)
}

type bufferReaderAt []byte

func (b bufferReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b)) {
		return 0, io.EOF
	}
	return copy(p, b[off:]), nil
}

type simpleBlock struct {
	cid  cid.Cid
	data []byte
}
