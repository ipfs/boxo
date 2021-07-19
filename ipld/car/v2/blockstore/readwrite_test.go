package blockstore_test

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-merkledag"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/stretchr/testify/assert"

	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	ipfsblockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car/v2/blockstore"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/internal/carv1"
)

var (
	rng                       = rand.New(rand.NewSource(1413))
	oneTestBlockWithCidV1     = merkledag.NewRawNode([]byte("fish")).Block
	anotherTestBlockWithCidV0 = blocks.NewBlock([]byte("barreleye"))
)

func TestReadWriteGetReturnsBlockstoreNotFoundWhenCidDoesNotExist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readwrite-err-not-found.car")
	subject, err := blockstore.OpenReadWrite(path, []cid.Cid{})
	t.Cleanup(func() { subject.Close() })
	require.NoError(t, err)
	nonExistingKey := merkledag.NewRawNode([]byte("undadasea")).Block.Cid()

	// Assert blockstore API returns blockstore.ErrNotFound
	gotBlock, err := subject.Get(nonExistingKey)
	require.Equal(t, ipfsblockstore.ErrNotFound, err)
	require.Nil(t, gotBlock)
}

func TestBlockstore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	f, err := os.Open("../testdata/sample-v1.car")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, f.Close()) })
	r, err := carv1.NewCarReader(f)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "readwrite.car")
	ingester, err := blockstore.OpenReadWrite(path, r.Header.Roots)
	require.NoError(t, err)
	t.Cleanup(func() { ingester.Finalize() })

	cids := make([]cid.Cid, 0)
	for {
		b, err := r.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		err = ingester.Put(b)
		require.NoError(t, err)
		cids = append(cids, b.Cid())

		// try reading a random one:
		candidate := cids[rng.Intn(len(cids))]
		if has, err := ingester.Has(candidate); !has || err != nil {
			t.Fatalf("expected to find %s but didn't: %s", candidate, err)
		}
	}

	for _, c := range cids {
		b, err := ingester.Get(c)
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
		b, err := robs.Get(c)
		require.NoError(t, err)
		if !b.Cid().Equals(c) {
			t.Fatal("wrong item returned")
		}
		numKeysCh++
	}
	if numKeysCh != len(cids) {
		t.Fatalf("AllKeysChan returned an unexpected amount of keys; expected %v but got %v", len(cids), numKeysCh)
	}

	for _, c := range cids {
		b, err := robs.Get(c)
		require.NoError(t, err)
		if !b.Cid().Equals(c) {
			t.Fatal("wrong item returned")
		}
	}
}

func TestBlockstorePutSameHashes(t *testing.T) {
	tdir := t.TempDir()

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
			has, err := bs.Has(block.Cid())
			require.NoError(t, err)
			if i == 0 {
				require.False(t, has)
			}

			err = bs.Put(block)
			require.NoError(t, err)

			// Has, Get, and GetSize need to work right after a Put.
			has, err = bs.Has(block.Cid())
			require.NoError(t, err)
			require.True(t, has)

			got, err := bs.Get(block.Cid())
			require.NoError(t, err)
			require.Equal(t, block.Cid(), got.Cid())
			require.Equal(t, block.RawData(), got.RawData())

			size, err := bs.GetSize(block.Cid())
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

			has, err := wbs.Has(block.Cid())
			require.NoError(t, err)
			require.False(t, has)

			err = wbs.Put(block)
			require.NoError(t, err)

			got, err := wbs.Get(block.Cid())
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
	paddedV1, err := ioutil.ReadFile("../testdata/sample-v1.car")
	require.NoError(t, err)

	// A sample null-padded CARv1 file.
	paddedV1 = append(paddedV1, make([]byte, 2048)...)

	rbs, err := blockstore.NewReadOnly(bufferReaderAt(paddedV1), nil,
		carv2.ZeroLengthSectionAsEOF(true))
	require.NoError(t, err)

	roots, err := rbs.Roots()
	require.NoError(t, err)

	has, err := rbs.Has(roots[0])
	require.NoError(t, err)
	require.True(t, has)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	allKeysCh, err := rbs.AllKeysChan(ctx)
	require.NoError(t, err)
	for c := range allKeysCh {
		b, err := rbs.Get(c)
		require.NoError(t, err)
		if !b.Cid().Equals(c) {
			t.Fatal("wrong item returned")
		}
	}
}

func TestBlockstoreResumption(t *testing.T) {
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
	var wantBlockCountSoFar int
	wantBlocks := make(map[cid.Cid]blocks.Block)
	for {
		b, err := r.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		wantBlockCountSoFar++
		wantBlocks[b.Cid()] = b

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
				require.NoError(t, subject.Close())
			}
			subject, err = blockstore.OpenReadWrite(path, r.Header.Roots,
				blockstore.UseWholeCIDs(true))
			require.NoError(t, err)
		}
		require.NoError(t, subject.Put(b))

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
				has, err := subject.Has(k)
				require.NoError(t, err)
				require.True(t, has)
				gotBlock, err := subject.Get(k)
				require.NoError(t, err)
				require.Equal(t, wantBlocks[k], gotBlock)
				gotBlockCountSoFar++
			}
			// Assert the number of blocks in file are as expected calculated via AllKeysChan
			require.Equal(t, wantBlockCountSoFar, gotBlockCountSoFar)
		}
	}
	require.NoError(t, subject.Close())

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

	gotPayloadReader, err := carv1.NewCarReader(v2r.DataReader())
	require.NoError(t, err)

	require.Equal(t, wantPayloadReader.Header, gotPayloadReader.Header)
	for {
		wantNextBlock, wantErr := wantPayloadReader.Next()
		gotNextBlock, gotErr := gotPayloadReader.Next()
		if wantErr == io.EOF {
			require.Equal(t, wantErr, gotErr)
			break
		}
		require.NoError(t, wantErr)
		require.NoError(t, gotErr)
		require.Equal(t, wantNextBlock, gotNextBlock)
	}

	// Assert index in resumed from file is identical to index generated directly from original CARv1 payload.
	_, err = v1f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	gotIdx, err := index.ReadFrom(v2r.IndexReader())
	require.NoError(t, err)
	wantIdx, err := carv2.GenerateIndex(v1f)
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
	t.Cleanup(func() { subject.Close() })
	require.NoError(t, err)
}

func TestReadWritePanicsOnlyWhenFinalized(t *testing.T) {
	oneTestBlockCid := oneTestBlockWithCidV1.Cid()
	anotherTestBlockCid := anotherTestBlockWithCidV0.Cid()
	wantRoots := []cid.Cid{oneTestBlockCid, anotherTestBlockCid}
	path := filepath.Join(t.TempDir(), "readwrite-finalized-panic.car")

	subject, err := blockstore.OpenReadWrite(path, wantRoots)
	require.NoError(t, err)
	t.Cleanup(func() { subject.Close() })

	require.NoError(t, subject.Put(oneTestBlockWithCidV1))
	require.NoError(t, subject.Put(anotherTestBlockWithCidV0))

	gotBlock, err := subject.Get(oneTestBlockCid)
	require.NoError(t, err)
	require.Equal(t, oneTestBlockWithCidV1, gotBlock)

	gotSize, err := subject.GetSize(oneTestBlockCid)
	require.NoError(t, err)
	require.Equal(t, len(oneTestBlockWithCidV1.RawData()), gotSize)

	gotRoots, err := subject.Roots()
	require.NoError(t, err)
	require.Equal(t, wantRoots, gotRoots)

	has, err := subject.Has(oneTestBlockCid)
	require.NoError(t, err)
	require.True(t, has)

	subject.HashOnRead(true)
	// Delete should always panic regardless of finalize
	require.Panics(t, func() { subject.DeleteBlock(oneTestBlockCid) })

	require.NoError(t, subject.Finalize())
	require.Panics(t, func() { subject.Get(oneTestBlockCid) })
	require.Panics(t, func() { subject.GetSize(anotherTestBlockCid) })
	require.Panics(t, func() { subject.Has(anotherTestBlockCid) })
	require.Panics(t, func() { subject.HashOnRead(true) })
	require.Panics(t, func() { subject.Put(oneTestBlockWithCidV1) })
	require.Panics(t, func() { subject.PutMany([]blocks.Block{anotherTestBlockWithCidV0}) })
	require.Panics(t, func() { subject.AllKeysChan(context.Background()) })
	require.Panics(t, func() { subject.DeleteBlock(oneTestBlockCid) })
}

func TestReadWriteWithPaddingWorksAsExpected(t *testing.T) {
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
	t.Cleanup(func() { subject.Close() })
	require.NoError(t, subject.Put(oneTestBlockWithCidV1))
	require.NoError(t, subject.Put(anotherTestBlockWithCidV0))
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
	subject, err := blockstore.OpenReadWrite("../testdata/sample-rootless-v42.car", []cid.Cid{})
	require.EqualError(t, err, "cannot resume on CAR file with version 42")
	require.Nil(t, subject)
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
	t.Cleanup(func() { subject.Close() })
	require.NoError(t, subject.Put(oneTestBlockWithCidV1))
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
