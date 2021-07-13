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

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/stretchr/testify/assert"

	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/ipld/go-car/v2/blockstore"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/internal/carv1"
)

func TestBlockstore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	f, err := os.Open("testdata/test.car")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, f.Close()) })
	r, err := carv1.NewCarReader(f)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "readwrite.car")
	ingester, err := blockstore.NewReadWrite(path, r.Header.Roots)
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
		candidate := cids[rand.Intn(len(cids))]
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
	wbs, err := blockstore.NewReadWrite(
		filepath.Join(tdir, "readwrite.car"), nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { wbs.Finalize() })

	wbsd, err := blockstore.NewReadWrite(
		filepath.Join(tdir, "readwrite-dedup.car"), nil,
		blockstore.WithCidDeduplication,
	)
	require.NoError(t, err)
	t.Cleanup(func() { wbsd.Finalize() })

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
	appendBlock(data1, 0, cid.Raw)
	appendBlock(data1, 1, cid.Raw)
	appendBlock(data1, 1, cid.DagCBOR)
	appendBlock(data1, 1, cid.DagCBOR) // duplicate CID

	data2 := []byte("foo bar baz")
	appendBlock(data2, 0, cid.Raw)
	appendBlock(data2, 1, cid.Raw)
	appendBlock(data2, 1, cid.Raw) // duplicate CID
	appendBlock(data2, 1, cid.DagCBOR)

	countBlocks := func(bs *blockstore.ReadWrite) int {
		ch, err := bs.AllKeysChan(context.Background())
		require.NoError(t, err)

		n := 0
		for range ch {
			n++
		}
		return n
	}

	for i, block := range blockList {
		// Has should never error here.
		// The first block should be missing.
		// Others might not, given the duplicate hashes.
		has, err := wbs.Has(block.Cid())
		require.NoError(t, err)
		if i == 0 {
			require.False(t, has)
		}

		err = wbs.Put(block)
		require.NoError(t, err)
	}

	for _, block := range blockList {
		has, err := wbs.Has(block.Cid())
		require.NoError(t, err)
		require.True(t, has)

		got, err := wbs.Get(block.Cid())
		require.NoError(t, err)
		require.Equal(t, block.Cid(), got.Cid())
		require.Equal(t, block.RawData(), got.RawData())
	}

	require.Equal(t, len(blockList), countBlocks(wbs))

	err = wbs.Finalize()
	require.NoError(t, err)

	// Put the same list of blocks to the blockstore that
	// deduplicates by CID.
	// We should end up with two fewer blocks.
	for _, block := range blockList {
		err = wbsd.Put(block)
		require.NoError(t, err)
	}
	require.Equal(t, len(blockList)-2, countBlocks(wbsd))

	err = wbsd.Finalize()
	require.NoError(t, err)
}

func TestBlockstoreConcurrentUse(t *testing.T) {
	wbs, err := blockstore.NewReadWrite(filepath.Join(t.TempDir(), "readwrite.car"), nil)
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
	paddedV1, err := ioutil.ReadFile("testdata/test.car")
	require.NoError(t, err)

	// A sample null-padded CARv1 file.
	paddedV1 = append(paddedV1, make([]byte, 2048)...)

	rbs, err := blockstore.NewReadOnly(bufferReaderAt(paddedV1), nil)
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
	v1f, err := os.Open("testdata/test.car")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, v1f.Close()) })
	r, err := carv1.NewCarReader(v1f)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "readwrite-resume.car")
	// Create an incomplete CAR v2 file with no blocks put.
	subject, err := blockstore.NewReadWrite(path, r.Header.Roots)
	require.NoError(t, err)

	// For each block resume on the same file, putting blocks one at a time.
	for {
		b, err := r.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		// 30% chance of subject failing (or closing file without calling Finalize).
		// The higher this percentage the slower the test runs considering the number of blocks in the original CAR v1 test payload.
		shouldFailAbruptly := rand.Float32() <= 0.3
		if shouldFailAbruptly {
			// Close off the open file and re-instantiate a new subject with resumption enabled.
			// Note, we don't have to close the file for resumption to work.
			// We do this to avoid resource leak during testing.
			require.NoError(t, subject.Close())
			subject, err = blockstore.NewReadWrite(path, r.Header.Roots)
			require.NoError(t, err)
		}
		require.NoError(t, subject.Put(b))
	}
	require.NoError(t, subject.Close())

	// Finalize the blockstore to complete partially written CAR v2 file.
	subject, err = blockstore.NewReadWrite(path, r.Header.Roots)
	require.NoError(t, err)
	require.NoError(t, subject.Finalize())

	// Assert resumed from file is a valid CAR v2 with index.
	v2f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, v2f.Close()) })
	v2r, err := carv2.NewReader(v2f)
	require.NoError(t, err)
	require.True(t, v2r.Header.HasIndex())

	// Assert CAR v1 payload in file matches the original CAR v1 payload.
	_, err = v1f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	wantPayloadReader, err := carv1.NewCarReader(v1f)
	require.NoError(t, err)

	gotPayloadReader, err := carv1.NewCarReader(v2r.CarV1Reader())
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

	// Assert index in resumed from file is identical to index generated directly from original CAR v1 payload.
	_, err = v1f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	gotIdx, err := index.ReadFrom(v2r.IndexReader())
	require.NoError(t, err)
	wantIdx, err := carv2.GenerateIndex(v1f)
	require.NoError(t, err)
	require.Equal(t, wantIdx, gotIdx)
}

func TestBlockstoreResumptionFailsOnFinalizedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readwrite-resume-finalized.car")
	// Create an incomplete CAR v2 file with no blocks put.
	subject, err := blockstore.NewReadWrite(path, []cid.Cid{})
	require.NoError(t, err)
	require.NoError(t, subject.Finalize())
	_, err = blockstore.NewReadWrite(path, []cid.Cid{})
	require.Errorf(t, err, "cannot resume from a finalized file")
}
