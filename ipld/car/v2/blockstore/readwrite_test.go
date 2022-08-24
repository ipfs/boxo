package blockstore_test

import (
	"context"
	"crypto/sha512"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	rng                       = rand.New(rand.NewSource(1413))
	oneTestBlockWithCidV1     = merkledag.NewRawNode([]byte("fish")).Block
	anotherTestBlockWithCidV0 = blocks.NewBlock([]byte("barreleye"))
)

func TestReadWriteGetReturnsBlockstoreNotFoundWhenCidDoesNotExist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readwrite-err-not-found.car")
	subject, err := blockstore.OpenReadWrite(path, []cid.Cid{})
	t.Cleanup(func() { subject.Finalize() })
	require.NoError(t, err)
	nonExistingKey := merkledag.NewRawNode([]byte("undadasea")).Block.Cid()

	// Assert blockstore API returns blockstore.ErrNotFound
	gotBlock, err := subject.Get(context.TODO(), nonExistingKey)
	require.IsType(t, format.ErrNotFound{}, err)
	require.Nil(t, gotBlock)
}

func TestBlockstoreX(t *testing.T) {
	originalCARv1Path := "../testdata/sample-v1.car"
	originalCARv1ComparePath := "../testdata/sample-v1-noidentity.car"
	originalCARv1ComparePathStat, err := os.Stat(originalCARv1ComparePath)
	require.NoError(t, err)

	variants := []struct {
		name                  string
		options               []carv2.Option
		expectedV1StartOffset int64
	}{
		// no options, expect a standard CARv2 with the noidentity inner CARv1
		{"noopt_carv2", []carv2.Option{}, int64(carv2.PragmaSize + carv2.HeaderSize)},
		// option to only write as a CARv1, expect the noidentity inner CARv1
		{"carv1", []carv2.Option{blockstore.WriteAsCarV1(true)}, int64(0)},
	}

	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			f, err := os.Open(originalCARv1Path)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, f.Close()) })
			r, err := carv1.NewCarReader(f)
			require.NoError(t, err)

			path := filepath.Join(t.TempDir(), fmt.Sprintf("readwrite_%s.car", variant.name))
			ingester, err := blockstore.OpenReadWrite(path, r.Header.Roots, variant.options...)
			require.NoError(t, err)
			t.Cleanup(func() { ingester.Finalize() })

			cids := make([]cid.Cid, 0)
			var idCidCount int
			for {
				b, err := r.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)

				err = ingester.Put(ctx, b)
				require.NoError(t, err)
				cids = append(cids, b.Cid())

				// try reading a random one:
				candidate := cids[rng.Intn(len(cids))]
				if has, err := ingester.Has(ctx, candidate); !has || err != nil {
					t.Fatalf("expected to find %s but didn't: %s", candidate, err)
				}

				dmh, err := multihash.Decode(b.Cid().Hash())
				require.NoError(t, err)
				if dmh.Code == multihash.IDENTITY {
					idCidCount++
				}
			}

			for _, c := range cids {
				b, err := ingester.Get(ctx, c)
				require.NoError(t, err)
				if !b.Cid().Equals(c) {
					t.Fatal("wrong item returned")
				}
			}

			err = ingester.Finalize()
			require.NoError(t, err)
			robs, err := blockstore.OpenReadOnly(path)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, robs.Close()) })

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
			expectedCidCount := len(cids) - idCidCount
			require.Equal(t, expectedCidCount, numKeysCh, "AllKeysChan returned an unexpected amount of keys; expected %v but got %v", expectedCidCount, numKeysCh)

			for _, c := range cids {
				b, err := robs.Get(ctx, c)
				require.NoError(t, err)
				if !b.Cid().Equals(c) {
					t.Fatal("wrong item returned")
				}
			}

			wrote, err := os.Open(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wrote.Close()) })
			_, err = wrote.Seek(variant.expectedV1StartOffset, io.SeekStart)
			require.NoError(t, err)
			hasher := sha512.New()
			gotWritten, err := io.Copy(hasher, io.LimitReader(wrote, originalCARv1ComparePathStat.Size()))
			require.NoError(t, err)
			gotSum := hasher.Sum(nil)

			hasher.Reset()
			originalCarV1, err := os.Open(originalCARv1ComparePath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, originalCarV1.Close()) })
			wantWritten, err := io.Copy(hasher, originalCarV1)
			require.NoError(t, err)
			wantSum := hasher.Sum(nil)

			require.Equal(t, wantWritten, gotWritten)
			require.Equal(t, wantSum, gotSum)
		})
	}
}

func TestBlockstorePutSameHashes(t *testing.T) {
	tdir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// This blockstore allows duplicate puts,
	// and identifies by multihash as per the default.
	wbsAllowDups, err := blockstore.OpenReadWrite(
		filepath.Join(tdir, "readwrite-allowdup.car"), nil,
		blockstore.AllowDuplicatePuts(true),
	)
	require.NoError(t, err)
	t.Cleanup(func() { wbsAllowDups.Finalize() })

	// This blockstore deduplicates puts by CID.
	wbsByCID, err := blockstore.OpenReadWrite(
		filepath.Join(tdir, "readwrite-dedup-wholecid.car"), nil,
		blockstore.UseWholeCIDs(true),
	)
	require.NoError(t, err)
	t.Cleanup(func() { wbsByCID.Finalize() })

	// This blockstore deduplicates puts by multihash.
	wbsByHash, err := blockstore.OpenReadWrite(
		filepath.Join(tdir, "readwrite-dedup-hash.car"), nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { wbsByHash.Finalize() })

	var blockList []blocks.Block

	appendBlock := func(data []byte, version, codec uint64) {
		c, err := cid.Prefix{
			Version:  version,
			Codec:    codec,
			MhType:   multihash.SHA2_256,
			MhLength: -1,
		}.Sum(data)
		require.NoError(t, err)

		block, err := blocks.NewBlockWithCid(data, c)
		require.NoError(t, err)

		blockList = append(blockList, block)
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

	countBlocks := func(bs *blockstore.ReadWrite) int {
		ch, err := bs.AllKeysChan(context.Background())
		require.NoError(t, err)

		n := 0
		for c := range ch {
			if c.Prefix().Codec == cid.Raw {
				if bs == wbsByCID {
					t.Error("expected blockstore with UseWholeCIDs to not flatten on AllKeysChan")
				}
			} else {
				if bs != wbsByCID {
					t.Error("expected blockstore without UseWholeCIDs to flatten on AllKeysChan")
				}
			}
			n++
		}
		return n
	}

	putBlockList := func(bs *blockstore.ReadWrite) {
		for i, block := range blockList {
			// Has should never error here.
			// The first block should be missing.
			// Others might not, given the duplicate hashes.
			has, err := bs.Has(ctx, block.Cid())
			require.NoError(t, err)
			if i == 0 {
				require.False(t, has)
			}

			err = bs.Put(ctx, block)
			require.NoError(t, err)

			// Has, Get, and GetSize need to work right after a Put.
			has, err = bs.Has(ctx, block.Cid())
			require.NoError(t, err)
			require.True(t, has)

			got, err := bs.Get(ctx, block.Cid())
			require.NoError(t, err)
			require.Equal(t, block.Cid(), got.Cid())
			require.Equal(t, block.RawData(), got.RawData())

			size, err := bs.GetSize(ctx, block.Cid())
			require.NoError(t, err)
			require.Equal(t, len(block.RawData()), size)
		}
	}

	putBlockList(wbsAllowDups)
	require.Equal(t, len(blockList), countBlocks(wbsAllowDups))

	err = wbsAllowDups.Finalize()
	require.NoError(t, err)

	// Put the same list of blocks to the blockstore that
	// deduplicates by CID.
	// We should end up with two fewer blocks,
	// as two are entire CID duplicates.
	putBlockList(wbsByCID)
	require.Equal(t, len(blockList)-2, countBlocks(wbsByCID))

	err = wbsByCID.Finalize()
	require.NoError(t, err)

	// Put the same list of blocks to the blockstore that
	// deduplicates by CID.
	// We should end up with just two blocks,
	// as the original set of blocks only has two distinct multihashes.
	putBlockList(wbsByHash)
	require.Equal(t, 2, countBlocks(wbsByHash))

	err = wbsByHash.Finalize()
	require.NoError(t, err)
}

func TestBlockstoreConcurrentUse(t *testing.T) {
	wbs, err := blockstore.OpenReadWrite(filepath.Join(t.TempDir(), "readwrite.car"), nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, err)
	t.Cleanup(func() { wbs.Finalize() })

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))

		wg.Add(1)
		go func() {
			defer wg.Done()

			c, err := cid.Prefix{
				Version:  1,
				Codec:    cid.Raw,
				MhType:   multihash.SHA2_256,
				MhLength: -1,
			}.Sum(data)
			require.NoError(t, err)

			block, err := blocks.NewBlockWithCid(data, c)
			require.NoError(t, err)

			has, err := wbs.Has(ctx, block.Cid())
			require.NoError(t, err)
			require.False(t, has)

			err = wbs.Put(ctx, block)
			require.NoError(t, err)

			got, err := wbs.Get(ctx, block.Cid())
			require.NoError(t, err)
			require.Equal(t, data, got.RawData())
		}()
	}
	wg.Wait()
}

type bufferReaderAt []byte

func (b bufferReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b)) {
		return 0, io.EOF
	}
	return copy(p, b[off:]), nil
}

func TestBlockstoreNullPadding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	paddedV1, err := os.ReadFile("../testdata/sample-v1-with-zero-len-section.car")
	require.NoError(t, err)

	rbs, err := blockstore.NewReadOnly(bufferReaderAt(paddedV1), nil,
		carv2.ZeroLengthSectionAsEOF(true))
	require.NoError(t, err)

	roots, err := rbs.Roots()
	require.NoError(t, err)

	has, err := rbs.Has(ctx, roots[0])
	require.NoError(t, err)
	require.True(t, has)

	allKeysCh, err := rbs.AllKeysChan(ctx)
	require.NoError(t, err)
	for c := range allKeysCh {
		b, err := rbs.Get(ctx, c)
		require.NoError(t, err)
		if !b.Cid().Equals(c) {
			t.Fatal("wrong item returned")
		}
	}
}

func TestBlockstoreResumption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	v1f, err := os.Open("../testdata/sample-v1.car")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, v1f.Close()) })
	r, err := carv1.NewCarReader(v1f)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "readwrite-resume.car")
	// Create an incomplete CARv2 file with no blocks put.
	subject, err := blockstore.OpenReadWrite(path, r.Header.Roots,
		blockstore.UseWholeCIDs(true))
	require.NoError(t, err)

	// For each block resume on the same file, putting blocks one at a time.
	var wantBlockCountSoFar, idCidCount int
	wantBlocks := make(map[cid.Cid]blocks.Block)
	for {
		b, err := r.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		wantBlockCountSoFar++
		wantBlocks[b.Cid()] = b

		dmh, err := multihash.Decode(b.Cid().Hash())
		require.NoError(t, err)
		if dmh.Code == multihash.IDENTITY {
			idCidCount++
		}

		// 30% chance of subject failing; more concretely: re-instantiating blockstore with the same
		// file without calling Finalize. The higher this percentage the slower the test runs
		// considering the number of blocks in the original CARv1 test payload.
		resume := rng.Float32() <= 0.3
		// If testing resume case, then flip a coin to decide whether to finalize before blockstore
		// re-instantiation or not. Note, both cases should work for resumption since we do not
		// limit resumption to unfinalized files.
		finalizeBeforeResumption := rng.Float32() <= 0.5
		if resume {
			if finalizeBeforeResumption {
				require.NoError(t, subject.Finalize())
			} else {
				// Close off the open file and re-instantiate a new subject with resumption enabled.
				// Note, we don't have to close the file for resumption to work.
				// We do this to avoid resource leak during testing.
				subject.Discard()
			}
			subject, err = blockstore.OpenReadWrite(path, r.Header.Roots,
				blockstore.UseWholeCIDs(true))
			require.NoError(t, err)
		}
		require.NoError(t, subject.Put(ctx, b))

		// With 10% chance test read operations on an resumed read-write blockstore.
		// We don't test on every put to reduce test runtime.
		testRead := rng.Float32() <= 0.1
		if testRead {
			// Assert read operations on the read-write blockstore are as expected when resumed from an
			// existing file
			var gotBlockCountSoFar int
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			t.Cleanup(cancel)
			keysChan, err := subject.AllKeysChan(ctx)
			require.NoError(t, err)
			for k := range keysChan {
				has, err := subject.Has(ctx, k)
				require.NoError(t, err)
				require.True(t, has)
				gotBlock, err := subject.Get(ctx, k)
				require.NoError(t, err)
				require.Equal(t, wantBlocks[k], gotBlock)
				gotBlockCountSoFar++
			}
			// Assert the number of blocks in file are as expected calculated via AllKeysChan
			require.Equal(t, wantBlockCountSoFar-idCidCount, gotBlockCountSoFar)
		}
	}
	subject.Discard()

	// Finalize the blockstore to complete partially written CARv2 file.
	subject, err = blockstore.OpenReadWrite(path, r.Header.Roots,
		blockstore.UseWholeCIDs(true))
	require.NoError(t, err)
	require.NoError(t, subject.Finalize())

	// Assert resumed from file is a valid CARv2 with index.
	v2f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, v2f.Close()) })
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

func TestBlockstoreResumptionIsSupportedOnFinalizedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readwrite-resume-finalized.car")
	// Create an incomplete CARv2 file with no blocks put.
	subject, err := blockstore.OpenReadWrite(path, []cid.Cid{})
	require.NoError(t, err)
	require.NoError(t, subject.Finalize())
	subject, err = blockstore.OpenReadWrite(path, []cid.Cid{})
	require.NoError(t, err)
	t.Cleanup(func() { subject.Finalize() })
}

func TestReadWritePanicsOnlyWhenFinalized(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	oneTestBlockCid := oneTestBlockWithCidV1.Cid()
	anotherTestBlockCid := anotherTestBlockWithCidV0.Cid()
	wantRoots := []cid.Cid{oneTestBlockCid, anotherTestBlockCid}
	path := filepath.Join(t.TempDir(), "readwrite-finalized-panic.car")

	subject, err := blockstore.OpenReadWrite(path, wantRoots)
	require.NoError(t, err)

	require.NoError(t, subject.Put(ctx, oneTestBlockWithCidV1))
	require.NoError(t, subject.Put(ctx, anotherTestBlockWithCidV0))

	gotBlock, err := subject.Get(ctx, oneTestBlockCid)
	require.NoError(t, err)
	require.Equal(t, oneTestBlockWithCidV1, gotBlock)

	gotSize, err := subject.GetSize(ctx, oneTestBlockCid)
	require.NoError(t, err)
	require.Equal(t, len(oneTestBlockWithCidV1.RawData()), gotSize)

	gotRoots, err := subject.Roots()
	require.NoError(t, err)
	require.Equal(t, wantRoots, gotRoots)

	has, err := subject.Has(ctx, oneTestBlockCid)
	require.NoError(t, err)
	require.True(t, has)

	subject.HashOnRead(true)
	// Delete should always error regardless of finalize
	require.Error(t, subject.DeleteBlock(ctx, oneTestBlockCid))

	require.NoError(t, subject.Finalize())
	require.Error(t, subject.Finalize())

	_, ok := (interface{})(subject).(io.Closer)
	require.False(t, ok)

	_, err = subject.Get(ctx, oneTestBlockCid)
	require.Error(t, err)
	_, err = subject.GetSize(ctx, anotherTestBlockCid)
	require.Error(t, err)
	_, err = subject.Has(ctx, anotherTestBlockCid)
	require.Error(t, err)

	require.Error(t, subject.Put(ctx, oneTestBlockWithCidV1))
	require.Error(t, subject.PutMany(ctx, []blocks.Block{anotherTestBlockWithCidV0}))
	_, err = subject.AllKeysChan(context.Background())
	require.Error(t, err)
	require.Error(t, subject.DeleteBlock(ctx, oneTestBlockCid))
}

func TestReadWriteWithPaddingWorksAsExpected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	oneTestBlockCid := oneTestBlockWithCidV1.Cid()
	anotherTestBlockCid := anotherTestBlockWithCidV0.Cid()
	WantRoots := []cid.Cid{oneTestBlockCid, anotherTestBlockCid}
	path := filepath.Join(t.TempDir(), "readwrite-with-padding.car")

	wantCarV1Padding := uint64(1413)
	wantIndexPadding := uint64(1314)
	subject, err := blockstore.OpenReadWrite(
		path,
		WantRoots,
		carv2.UseDataPadding(wantCarV1Padding),
		carv2.UseIndexPadding(wantIndexPadding))
	require.NoError(t, err)
	require.NoError(t, subject.Put(ctx, oneTestBlockWithCidV1))
	require.NoError(t, subject.Put(ctx, anotherTestBlockWithCidV0))
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
	require.Equal(t, WantRoots, gotCarV1.Header.Roots)
	gotOneBlock, err := gotCarV1.Next()
	require.NoError(t, err)
	require.Equal(t, oneTestBlockWithCidV1, gotOneBlock)
	gotAnotherBlock, err := gotCarV1.Next()
	require.NoError(t, err)
	require.Equal(t, anotherTestBlockWithCidV0, gotAnotherBlock)
	_, err = gotCarV1.Next()
	require.Equal(t, io.EOF, err)

	// Assert reading index directly from file is parsable and has expected CIDs.
	stat, err := f.Stat()
	require.NoError(t, err)
	indexSize := stat.Size() - int64(wantIndexOffset)
	gotIdx, err := index.ReadFrom(io.NewSectionReader(f, int64(wantIndexOffset), indexSize))
	require.NoError(t, err)
	_, err = index.GetFirst(gotIdx, oneTestBlockCid)
	require.NoError(t, err)
	_, err = index.GetFirst(gotIdx, anotherTestBlockCid)
	require.NoError(t, err)
}

func TestReadWriteResumptionFromNonV2FileIsError(t *testing.T) {
	tmpPath := requireTmpCopy(t, "../testdata/sample-rootless-v42.car")
	subject, err := blockstore.OpenReadWrite(tmpPath, []cid.Cid{})
	require.EqualError(t, err, "cannot resume on CAR file with version 42")
	require.Nil(t, subject)
}

func TestReadWriteResumptionMismatchingRootsIsError(t *testing.T) {
	tmpPath := requireTmpCopy(t, "../testdata/sample-wrapped-v2.car")

	origContent, err := os.ReadFile(tmpPath)
	require.NoError(t, err)

	badRoot, err := cid.NewPrefixV1(cid.Raw, multihash.SHA2_256).Sum([]byte("bad root"))
	require.NoError(t, err)

	subject, err := blockstore.OpenReadWrite(tmpPath, []cid.Cid{badRoot})
	require.EqualError(t, err, "cannot resume on file with mismatching data header")
	require.Nil(t, subject)

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
	oneTestBlockCid := oneTestBlockWithCidV1.Cid()
	WantRoots := []cid.Cid{oneTestBlockCid}
	path := filepath.Join(t.TempDir(), "readwrite-resume-with-padding.car")

	subject, err := blockstore.OpenReadWrite(
		path,
		WantRoots,
		carv2.UseDataPadding(1413))
	require.NoError(t, err)
	require.NoError(t, subject.Put(context.TODO(), oneTestBlockWithCidV1))
	require.NoError(t, subject.Finalize())

	resumingSubject, err := blockstore.OpenReadWrite(
		path,
		WantRoots,
		carv2.UseDataPadding(1314))
	require.EqualError(t, err, "cannot resume from file with mismatched CARv1 offset; "+
		"`WithDataPadding` option must match the padding on file. "+
		"Expected padding value of 1413 but got 1314")
	require.Nil(t, resumingSubject)
}

func TestReadWriteErrorAfterClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	root := blocks.NewBlock([]byte("foo"))
	for _, closeMethod := range []func(*blockstore.ReadWrite){
		(*blockstore.ReadWrite).Discard,
		func(bs *blockstore.ReadWrite) { bs.Finalize() },
	} {
		path := filepath.Join(t.TempDir(), "readwrite.car")
		bs, err := blockstore.OpenReadWrite(path, []cid.Cid{root.Cid()})
		require.NoError(t, err)

		err = bs.Put(ctx, root)
		require.NoError(t, err)

		roots, err := bs.Roots()
		require.NoError(t, err)
		_, err = bs.Has(ctx, roots[0])
		require.NoError(t, err)
		_, err = bs.Get(ctx, roots[0])
		require.NoError(t, err)
		_, err = bs.GetSize(ctx, roots[0])
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		_, err = bs.AllKeysChan(ctx)
		require.NoError(t, err)
		cancel() // to stop the AllKeysChan goroutine

		closeMethod(bs)

		_, err = bs.Roots()
		require.Error(t, err)
		_, err = bs.Has(ctx, roots[0])
		require.Error(t, err)
		_, err = bs.Get(ctx, roots[0])
		require.Error(t, err)
		_, err = bs.GetSize(ctx, roots[0])
		require.Error(t, err)
		_, err = bs.AllKeysChan(ctx)
		require.Error(t, err)

		err = bs.Put(ctx, root)
		require.Error(t, err)

		// TODO: test that closing blocks if an AllKeysChan operation is
		// in progress.
	}
}

func TestOpenReadWrite_WritesIdentityCIDsWhenOptionIsEnabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	path := filepath.Join(t.TempDir(), "readwrite-with-id-enabled.car")
	subject, err := blockstore.OpenReadWrite(path, []cid.Cid{}, carv2.StoreIdentityCIDs(true))
	require.NoError(t, err)

	data := []byte("fish")
	idmh, err := multihash.Sum(data, multihash.IDENTITY, -1)
	require.NoError(t, err)
	idCid := cid.NewCidV1(uint64(multicodec.Raw), idmh)

	idBlock, err := blocks.NewBlockWithCid(data, idCid)
	require.NoError(t, err)
	err = subject.Put(ctx, idBlock)
	require.NoError(t, err)

	has, err := subject.Has(ctx, idCid)
	require.NoError(t, err)
	require.True(t, has)

	gotBlock, err := subject.Get(ctx, idCid)
	require.NoError(t, err)
	require.Equal(t, idBlock, gotBlock)

	keysChan, err := subject.AllKeysChan(context.Background())
	require.NoError(t, err)
	var i int
	for c := range keysChan {
		i++
		require.Equal(t, idCid, c)
	}
	require.Equal(t, 1, i)

	err = subject.Finalize()
	require.NoError(t, err)

	// Assert resulting CAR file indeed has the IDENTITY block.
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	reader, err := carv2.NewBlockReader(f)
	require.NoError(t, err)

	gotBlock, err = reader.Next()
	require.NoError(t, err)
	require.Equal(t, idBlock, gotBlock)

	next, err := reader.Next()
	require.Equal(t, io.EOF, err)
	require.Nil(t, next)

	// Assert the id is indexed.
	r, err := carv2.OpenReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	require.True(t, r.Header.HasIndex())

	ir, err := r.IndexReader()
	require.NoError(t, err)
	require.NotNil(t, ir)

	gotIdx, err := index.ReadFrom(ir)
	require.NoError(t, err)

	// Determine expected offset as the length of header plus one
	dr, err := r.DataReader()
	require.NoError(t, err)
	header, err := carv1.ReadHeader(dr, carv1.DefaultMaxAllowedHeaderSize)
	require.NoError(t, err)
	object, err := cbor.DumpObject(header)
	require.NoError(t, err)
	expectedOffset := len(object) + 1

	// Assert index is iterable and has exactly one record with expected multihash and offset.
	switch idx := gotIdx.(type) {
	case index.IterableIndex:
		var i int
		err := idx.ForEach(func(mh multihash.Multihash, offset uint64) error {
			i++
			require.Equal(t, idmh, mh)
			require.Equal(t, uint64(expectedOffset), offset)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, i)
	default:
		require.Failf(t, "unexpected index type", "wanted %v but got %v", multicodec.CarMultihashIndexSorted, idx.Codec())
	}
}

func TestOpenReadWrite_ErrorsWhenWritingTooLargeOfACid(t *testing.T) {
	maxAllowedCidSize := uint64(2)
	path := filepath.Join(t.TempDir(), "readwrite-with-id-enabled-too-large.car")
	subject, err := blockstore.OpenReadWrite(path, []cid.Cid{}, carv2.MaxIndexCidSize(maxAllowedCidSize))
	t.Cleanup(subject.Discard)
	require.NoError(t, err)

	data := []byte("monsterlobster")
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	require.NoError(t, err)
	bigCid := cid.NewCidV1(uint64(multicodec.Raw), mh)
	bigCidLen := uint64(bigCid.ByteLen())
	require.True(t, bigCidLen > maxAllowedCidSize)

	bigBlock, err := blocks.NewBlockWithCid(data, bigCid)
	require.NoError(t, err)
	err = subject.Put(context.TODO(), bigBlock)
	require.Equal(t, &carv2.ErrCidTooLarge{MaxSize: maxAllowedCidSize, CurrentSize: bigCidLen}, err)
}

func TestReadWrite_ReWritingCARv1WithIdentityCidIsIdenticalToOriginalWithOptionsEnabled(t *testing.T) {
	originalCARv1Path := "../testdata/sample-v1.car"
	originalCarV1, err := os.Open(originalCARv1Path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, originalCarV1.Close()) })

	r, err := carv2.NewBlockReader(originalCarV1)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "readwrite-from-carv1-with-id-enabled.car")
	subject, err := blockstore.OpenReadWrite(path, r.Roots, carv2.StoreIdentityCIDs(true))
	require.NoError(t, err)
	var idCidCount int
	for {
		next, err := r.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if next.Cid().Prefix().MhType == multihash.IDENTITY {
			idCidCount++
		}
		err = subject.Put(context.TODO(), next)
		require.NoError(t, err)
	}
	require.NotZero(t, idCidCount)
	err = subject.Finalize()
	require.NoError(t, err)

	v2r, err := carv2.OpenReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, v2r.Close()) })

	// Assert characteristics bit is set.
	require.True(t, v2r.Header.Characteristics.IsFullyIndexed())

	// Assert original CARv1 and generated innter CARv1 payload have the same SHA512 hash
	// Note, we hash instead of comparing bytes to avoid excessive memory usage when sample CARv1 is large.

	hasher := sha512.New()
	dr, err := v2r.DataReader()
	require.NoError(t, err)
	gotWritten, err := io.Copy(hasher, dr)
	require.NoError(t, err)
	gotSum := hasher.Sum(nil)

	hasher.Reset()
	_, err = originalCarV1.Seek(0, io.SeekStart)
	require.NoError(t, err)
	wantWritten, err := io.Copy(hasher, originalCarV1)
	require.NoError(t, err)
	wantSum := hasher.Sum(nil)

	require.Equal(t, wantWritten, gotWritten)
	require.Equal(t, wantSum, gotSum)
}

func TestReadWriteOpenFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	dir := t.TempDir() // auto cleanup
	f, err := os.CreateTemp(dir, "")
	require.NoError(t, err)

	root := blocks.NewBlock([]byte("foo"))

	bs, err := blockstore.OpenReadWriteFile(f, []cid.Cid{root.Cid()})
	require.NoError(t, err)

	err = bs.Put(ctx, root)
	require.NoError(t, err)

	roots, err := bs.Roots()
	require.NoError(t, err)
	_, err = bs.Has(ctx, roots[0])
	require.NoError(t, err)
	_, err = bs.Get(ctx, roots[0])
	require.NoError(t, err)
	_, err = bs.GetSize(ctx, roots[0])
	require.NoError(t, err)

	err = bs.Finalize()
	require.NoError(t, err)

	_, err = f.Seek(0, 0)
	require.NoError(t, err) // file should not be closed, let the caller do it

	err = f.Close()
	require.NoError(t, err)
}

func TestBlockstore_IdentityCidWithEmptyDataIsIndexed(t *testing.T) {
	p := path.Join(t.TempDir(), "car-id-cid-empty.carv2")
	var noData []byte

	mh, err := multihash.Sum(noData, multihash.IDENTITY, -1)
	require.NoError(t, err)
	w, err := blockstore.OpenReadWrite(p, nil, carv2.StoreIdentityCIDs(true))
	require.NoError(t, err)

	blk, err := blocks.NewBlockWithCid(noData, cid.NewCidV1(cid.Raw, mh))
	require.NoError(t, err)

	err = w.Put(context.TODO(), blk)
	require.NoError(t, err)
	require.NoError(t, w.Finalize())

	r, err := carv2.OpenReader(p)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	dr, err := r.DataReader()
	require.NoError(t, err)
	header, err := carv1.ReadHeader(dr, carv1.DefaultMaxAllowedHeaderSize)
	require.NoError(t, err)
	wantOffset, err := carv1.HeaderSize(header)
	require.NoError(t, err)

	ir, err := r.IndexReader()
	require.NoError(t, err)
	idx, err := index.ReadFrom(ir)
	require.NoError(t, err)

	itidx, ok := idx.(index.IterableIndex)
	require.True(t, ok)
	var count int
	err = itidx.ForEach(func(m multihash.Multihash, u uint64) error {
		dm, err := multihash.Decode(m)
		require.NoError(t, err)
		require.Equal(t, multicodec.Identity, multicodec.Code(dm.Code))
		require.Equal(t, 0, dm.Length)
		require.Empty(t, dm.Digest)
		require.Equal(t, wantOffset, u)
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
}
