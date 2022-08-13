package decision

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/internal/testutil"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-metrics-interface"

	blocks "github.com/ipfs/go-block-format"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/delayed"
	ds_sync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	delay "github.com/ipfs/go-ipfs-delay"
)

func newBlockstoreManagerForTesting(
	t *testing.T,
	ctx context.Context,
	bs blockstore.Blockstore,
	workerCount int,
) *blockstoreManager {
	testPendingBlocksGauge := metrics.NewCtx(ctx, "pending_block_tasks", "Total number of pending blockstore tasks").Gauge()
	testActiveBlocksGauge := metrics.NewCtx(ctx, "active_block_tasks", "Total number of active blockstore tasks").Gauge()
	bsm := newBlockstoreManager(bs, workerCount, testPendingBlocksGauge, testActiveBlocksGauge)
	bsm.start()
	t.Cleanup(bsm.stop)
	return bsm
}

func TestBlockstoreManagerNotFoundKey(t *testing.T) {
	ctx := context.Background()
	bsdelay := delay.Fixed(3 * time.Millisecond)
	dstore := ds_sync.MutexWrap(delayed.New(ds.NewMapDatastore(), bsdelay))
	bstore := blockstore.NewBlockstore(ds_sync.MutexWrap(dstore))

	bsm := newBlockstoreManagerForTesting(t, ctx, bstore, 5)

	cids := testutil.GenerateCids(4)
	sizes, err := bsm.getBlockSizes(ctx, cids)
	if err != nil {
		t.Fatal(err)
	}
	if len(sizes) != 0 {
		t.Fatal("Wrong response length")
	}

	for _, c := range cids {
		if _, ok := sizes[c]; ok {
			t.Fatal("Non-existent block should have no size")
		}
	}

	blks, err := bsm.getBlocks(ctx, cids)
	if err != nil {
		t.Fatal(err)
	}
	if len(blks) != 0 {
		t.Fatal("Wrong response length")
	}

	for _, c := range cids {
		if _, ok := blks[c]; ok {
			t.Fatal("Non-existent block should have no size")
		}
	}
}

func TestBlockstoreManager(t *testing.T) {
	ctx := context.Background()
	bsdelay := delay.Fixed(3 * time.Millisecond)
	dstore := ds_sync.MutexWrap(delayed.New(ds.NewMapDatastore(), bsdelay))
	bstore := blockstore.NewBlockstore(ds_sync.MutexWrap(dstore))

	bsm := newBlockstoreManagerForTesting(t, ctx, bstore, 5)

	exp := make(map[cid.Cid]blocks.Block)
	var blks []blocks.Block
	for i := 0; i < 32; i++ {
		buf := make([]byte, 1024*(i+1))
		_, _ = rand.Read(buf)
		b := blocks.NewBlock(buf)
		blks = append(blks, b)
		exp[b.Cid()] = b
	}

	// Put all blocks in the blockstore except the last one
	if err := bstore.PutMany(ctx, blks[:len(blks)-1]); err != nil {
		t.Fatal(err)
	}

	var cids []cid.Cid
	for _, b := range blks {
		cids = append(cids, b.Cid())
	}

	sizes, err := bsm.getBlockSizes(ctx, cids)
	if err != nil {
		t.Fatal(err)
	}
	if len(sizes) != len(blks)-1 {
		t.Fatal("Wrong response length")
	}

	for _, c := range cids {
		expSize := len(exp[c].RawData())
		size, ok := sizes[c]

		// Only the last key should be missing
		if c.Equals(cids[len(cids)-1]) {
			if ok {
				t.Fatal("Non-existent block should not be in sizes map")
			}
		} else {
			if !ok {
				t.Fatal("Block should be in sizes map")
			}
			if size != expSize {
				t.Fatal("Block has wrong size")
			}
		}
	}

	fetched, err := bsm.getBlocks(ctx, cids)
	if err != nil {
		t.Fatal(err)
	}
	if len(fetched) != len(blks)-1 {
		t.Fatal("Wrong response length")
	}

	for _, c := range cids {
		blk, ok := fetched[c]

		// Only the last key should be missing
		if c.Equals(cids[len(cids)-1]) {
			if ok {
				t.Fatal("Non-existent block should not be in blocks map")
			}
		} else {
			if !ok {
				t.Fatal("Block should be in blocks map")
			}
			if !blk.Cid().Equals(c) {
				t.Fatal("Block has wrong cid")
			}
		}
	}
}

func TestBlockstoreManagerConcurrency(t *testing.T) {
	ctx := context.Background()
	bsdelay := delay.Fixed(3 * time.Millisecond)
	dstore := ds_sync.MutexWrap(delayed.New(ds.NewMapDatastore(), bsdelay))
	bstore := blockstore.NewBlockstore(ds_sync.MutexWrap(dstore))

	workerCount := 5
	bsm := newBlockstoreManagerForTesting(t, ctx, bstore, workerCount)

	blkSize := int64(8 * 1024)
	blks := testutil.GenerateBlocksOfSize(32, blkSize)
	var ks []cid.Cid
	for _, b := range blks {
		ks = append(ks, b.Cid())
	}

	err := bstore.PutMany(ctx, blks)
	if err != nil {
		t.Fatal(err)
	}

	// Create more concurrent requests than the number of workers
	wg := sync.WaitGroup{}
	for i := 0; i < 16; i++ {
		wg.Add(1)

		go func(t *testing.T) {
			defer wg.Done()

			sizes, err := bsm.getBlockSizes(ctx, ks)
			if err != nil {
				t.Error(err)
			}
			if len(sizes) != len(blks) {
				t.Error("Wrong response length")
			}
		}(t)
	}
	wg.Wait()
}

func TestBlockstoreManagerClose(t *testing.T) {
	ctx := context.Background()
	delayTime := 20 * time.Millisecond
	bsdelay := delay.Fixed(delayTime)
	dstore := ds_sync.MutexWrap(delayed.New(ds.NewMapDatastore(), bsdelay))
	bstore := blockstore.NewBlockstore(ds_sync.MutexWrap(dstore))

	bsm := newBlockstoreManagerForTesting(t, ctx, bstore, 3)

	blks := testutil.GenerateBlocksOfSize(10, 1024)
	var ks []cid.Cid
	for _, b := range blks {
		ks = append(ks, b.Cid())
	}

	err := bstore.PutMany(ctx, blks)
	if err != nil {
		t.Fatal(err)
	}

	bsm.stop()

	time.Sleep(5 * time.Millisecond)

	before := time.Now()
	_, err = bsm.getBlockSizes(ctx, ks)
	if err == nil {
		t.Error("expected an error")
	}
	// would expect to wait delayTime*10 if we didn't cancel.
	if time.Since(before) > delayTime*2 {
		t.Error("expected a fast timeout")
	}
}

func TestBlockstoreManagerCtxDone(t *testing.T) {
	delayTime := 20 * time.Millisecond
	bsdelay := delay.Fixed(delayTime)

	underlyingDstore := ds_sync.MutexWrap(ds.NewMapDatastore())
	dstore := delayed.New(underlyingDstore, bsdelay)
	underlyingBstore := blockstore.NewBlockstore(underlyingDstore)
	bstore := blockstore.NewBlockstore(dstore)

	ctx := context.Background()
	bsm := newBlockstoreManagerForTesting(t, ctx, bstore, 3)

	blks := testutil.GenerateBlocksOfSize(100, 128)
	var ks []cid.Cid
	for _, b := range blks {
		ks = append(ks, b.Cid())
	}

	err := underlyingBstore.PutMany(ctx, blks)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), delayTime/2)
	defer cancel()

	before := time.Now()
	_, err = bsm.getBlockSizes(ctx, ks)
	if err == nil {
		t.Error("expected an error")
	}

	// would expect to wait delayTime*100/3 if we didn't cancel.
	if time.Since(before) > delayTime*10 {
		t.Error("expected a fast timeout")
	}
}
