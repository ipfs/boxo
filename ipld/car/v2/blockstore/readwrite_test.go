package blockstore_test

import (
	"context"
	"io"
	"math/rand"
	"os"
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
	defer f.Close()
	r, err := carv1.NewCarReader(f)
	require.NoError(t, err)
	path := "testv2blockstore.car"
	ingester, err := blockstore.NewReadWrite(path, r.Header.Roots)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.Remove(path)
	}()

	cids := make([]cid.Cid, 0)
	for {
		b, err := r.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		if err := ingester.Put(b); err != nil {
			t.Fatal(err)
		}
		cids = append(cids, b.Cid())

		// try reading a random one:
		candidate := cids[rand.Intn(len(cids))]
		if has, err := ingester.Has(candidate); !has || err != nil {
			t.Fatalf("expected to find %s but didn't: %s", candidate, err)
		}
	}

	for _, c := range cids {
		b, err := ingester.Get(c)
		if err != nil {
			t.Fatal(err)
		}
		if !b.Cid().Equals(c) {
			t.Fatal("wrong item returned")
		}
	}

	if err := ingester.Finalize(); err != nil {
		t.Fatal(err)
	}
	carb, err := blockstore.OpenReadOnly(path, false)
	if err != nil {
		t.Fatal(err)
	}

	allKeysCh, err := carb.AllKeysChan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	numKeysCh := 0
	for c := range allKeysCh {
		b, err := carb.Get(c)
		if err != nil {
			t.Fatal(err)
		}
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
		if err != nil {
			t.Fatal(err)
		}
		if !b.Cid().Equals(c) {
			t.Fatal("wrong item returned")
		}
	}
}

func TestBlockstorePutSameHashes(t *testing.T) {
	path := "testv2blockstore.car"
	wbs, err := blockstore.NewReadWrite(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { os.Remove(path) }()

	var blockList []blocks.Block

	addBlock := func(data []byte, version, codec uint64) {
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

	data1 := []byte("foo bar")
	addBlock(data1, 0, cid.Raw)
	addBlock(data1, 1, cid.Raw)
	addBlock(data1, 1, cid.DagCBOR)

	data2 := []byte("foo bar baz")
	addBlock(data2, 0, cid.Raw)
	addBlock(data2, 1, cid.Raw)
	addBlock(data2, 1, cid.DagCBOR)

	for _, block := range blockList {
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

	err = wbs.Finalize()
	require.NoError(t, err)
}
