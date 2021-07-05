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
	t.Cleanup(func() { f.Close() })
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
	carb, err := blockstore.OpenReadOnly(path)
	require.NoError(t, err)
	t.Cleanup(func() { carb.Close() })

	allKeysCh, err := carb.AllKeysChan(ctx)
	require.NoError(t, err)
	numKeysCh := 0
	for c := range allKeysCh {
		b, err := carb.Get(c)
		require.NoError(t, err)
		if !b.Cid().Equals(c) {
			t.Fatal("wrong item returned")
		}
		numKeysCh++
	}
	if numKeysCh != len(cids) {
		t.Fatal("AllKeysChan returned an unexpected amount of keys")
	}

	for _, c := range cids {
		b, err := carb.Get(c)
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
