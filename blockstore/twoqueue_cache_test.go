package blockstore

import (
	"context"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	syncds "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
)

var exampleBlock = blocks.NewBlock([]byte("foo"))

func testTwoQueueCached(ctx context.Context, bs Blockstore) (*tqcache, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	opts := DefaultCacheOpts()
	opts.HasBloomFilterSize = 0
	opts.HasBloomFilterHashes = 0
	bbs, err := CachedBlockstore(ctx, bs, opts)
	if err == nil {
		return bbs.(*tqcache), nil
	}
	return nil, err
}

func createStores(t testing.TB) (*tqcache, Blockstore, *callbackDatastore) {
	cd := &callbackDatastore{f: func() {}, ds: ds.NewMapDatastore()}
	bs := NewBlockstore(syncds.MutexWrap(cd))
	c, err := testTwoQueueCached(context.TODO(), bs)
	if err != nil {
		t.Fatal(err)
	}
	return c, bs, cd
}

func trap(message string, cd *callbackDatastore, t *testing.T) {
	cd.SetFunc(func() {
		t.Fatal(message)
	})
}

func untrap(cd *callbackDatastore) {
	cd.SetFunc(func() {})
}

func TestRemoveCacheEntryOnDelete(t *testing.T) {
	c, _, cd := createStores(t)

	c.Put(bg, exampleBlock)

	cd.Lock()
	writeHitTheDatastore := false
	cd.Unlock()

	cd.SetFunc(func() {
		writeHitTheDatastore = true
	})

	c.DeleteBlock(bg, exampleBlock.Cid())
	c.Put(bg, exampleBlock)
	if !writeHitTheDatastore {
		t.Fail()
	}
}

func TestElideDuplicateWrite(t *testing.T) {
	c, _, cd := createStores(t)

	c.Put(bg, exampleBlock)
	trap("write hit datastore", cd, t)
	c.Put(bg, exampleBlock)
}

func TestHasRequestTriggersCache(t *testing.T) {
	c, _, cd := createStores(t)

	c.Has(bg, exampleBlock.Cid())
	trap("has hit datastore", cd, t)
	if has, err := c.Has(bg, exampleBlock.Cid()); has || err != nil {
		t.Fatal("has was true but there is no such block")
	}

	untrap(cd)
	err := c.Put(bg, exampleBlock)
	if err != nil {
		t.Fatal(err)
	}

	trap("has hit datastore", cd, t)

	if has, err := c.Has(bg, exampleBlock.Cid()); !has || err != nil {
		t.Fatal("has returned invalid result")
	}
}

func TestGetFillsCache(t *testing.T) {
	c, _, cd := createStores(t)

	if bl, err := c.Get(bg, exampleBlock.Cid()); bl != nil || err == nil {
		t.Fatal("block was found or there was no error")
	}

	trap("has hit datastore", cd, t)

	if has, err := c.Has(bg, exampleBlock.Cid()); has || err != nil {
		t.Fatal("has was true but there is no such block")
	}
	if _, err := c.GetSize(bg, exampleBlock.Cid()); !ipld.IsNotFound(err) {
		t.Fatal("getsize was true but there is no such block")
	}

	untrap(cd)

	if err := c.Put(bg, exampleBlock); err != nil {
		t.Fatal(err)
	}

	trap("has hit datastore", cd, t)

	if has, err := c.Has(bg, exampleBlock.Cid()); !has || err != nil {
		t.Fatal("has returned invalid result")
	}
	if blockSize, err := c.GetSize(bg, exampleBlock.Cid()); blockSize == -1 || err != nil {
		t.Fatal("getsize returned invalid result", blockSize, err)
	}
}

func TestGetAndDeleteFalseShortCircuit(t *testing.T) {
	c, _, cd := createStores(t)

	c.Has(bg, exampleBlock.Cid())
	c.GetSize(bg, exampleBlock.Cid())

	trap("get hit datastore", cd, t)

	if bl, err := c.Get(bg, exampleBlock.Cid()); bl != nil || !ipld.IsNotFound(err) {
		t.Fatal("get returned invalid result")
	}

	if c.DeleteBlock(bg, exampleBlock.Cid()) != nil {
		t.Fatal("expected deletes to be idempotent")
	}
}

func TestCacheCreationFailure(t *testing.T) {
	if c, err := newTwoQueueCachedBS(context.TODO(), nil, -1); c != nil || err == nil {
		t.Fatal("expected error and no cache")
	}
}

func TestInvalidKey(t *testing.T) {
	c, _, _ := createStores(t)

	bl, err := c.Get(bg, cid.Cid{})

	if bl != nil {
		t.Fatal("blocks should be nil")
	}
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestHasAfterSucessfulGetIsCached(t *testing.T) {
	c, bs, cd := createStores(t)

	bs.Put(bg, exampleBlock)

	c.Get(bg, exampleBlock.Cid())

	trap("has hit datastore", cd, t)
	c.Has(bg, exampleBlock.Cid())
}

func TestGetSizeAfterSucessfulGetIsCached(t *testing.T) {
	c, bs, cd := createStores(t)

	bs.Put(bg, exampleBlock)

	c.Get(bg, exampleBlock.Cid())

	trap("has hit datastore", cd, t)
	c.GetSize(bg, exampleBlock.Cid())
}

func TestGetSizeAfterSucessfulHas(t *testing.T) {
	c, bs, _ := createStores(t)

	bs.Put(bg, exampleBlock)
	has, err := c.Has(bg, exampleBlock.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("expected to have block")
	}

	if size, err := c.GetSize(bg, exampleBlock.Cid()); err != nil {
		t.Fatal(err)
	} else if size != len(exampleBlock.RawData()) {
		t.Fatalf("expected size %d, got %d", len(exampleBlock.RawData()), size)
	}
}

func TestGetSizeMissingZeroSizeBlock(t *testing.T) {
	c, bs, cd := createStores(t)
	emptyBlock := blocks.NewBlock([]byte{})
	missingBlock := blocks.NewBlock([]byte("missingBlock"))

	bs.Put(bg, emptyBlock)

	c.Get(bg, emptyBlock.Cid())

	trap("has hit datastore", cd, t)
	if blockSize, err := c.GetSize(bg, emptyBlock.Cid()); blockSize != 0 || err != nil {
		t.Fatal("getsize returned invalid result")
	}
	untrap(cd)

	c.Get(bg, missingBlock.Cid())

	trap("has hit datastore", cd, t)
	if _, err := c.GetSize(bg, missingBlock.Cid()); !ipld.IsNotFound(err) {
		t.Fatal("getsize returned invalid result")
	}
}

func TestDifferentKeyObjectsWork(t *testing.T) {
	c, bs, cd := createStores(t)

	bs.Put(bg, exampleBlock)

	c.Get(bg, exampleBlock.Cid())

	trap("has hit datastore", cd, t)
	cidstr := exampleBlock.Cid().String()

	ncid, err := cid.Decode(cidstr)
	if err != nil {
		t.Fatal(err)
	}

	c.Has(bg, ncid)
}

func TestPutManyCaches(t *testing.T) {
	t.Run("happy path PutMany", func(t *testing.T) {
		c, _, cd := createStores(t)
		c.PutMany(bg, []blocks.Block{exampleBlock})

		trap("has hit datastore", cd, t)
		c.Has(bg, exampleBlock.Cid())
		c.GetSize(bg, exampleBlock.Cid())
		untrap(cd)
		c.DeleteBlock(bg, exampleBlock.Cid())

		c.Put(bg, exampleBlock)
		trap("PunMany has hit datastore", cd, t)
		c.PutMany(bg, []blocks.Block{exampleBlock})
	})

	t.Run("PutMany with duplicates", func(t *testing.T) {
		c, _, cd := createStores(t)
		c.PutMany(bg, []blocks.Block{exampleBlock, exampleBlock})

		trap("has hit datastore", cd, t)
		c.Has(bg, exampleBlock.Cid())
		c.GetSize(bg, exampleBlock.Cid())
		untrap(cd)
		c.DeleteBlock(bg, exampleBlock.Cid())

		c.Put(bg, exampleBlock)
		trap("PunMany has hit datastore", cd, t)
		c.PutMany(bg, []blocks.Block{exampleBlock})
	})
}

func BenchmarkTwoQueueCacheConcurrentOps(b *testing.B) {
	// ~4k blocks seems high enough to be realistic,
	// but low enough to cause collisions.
	// Keep it as a power of 2, to simplify code below.
	const numBlocks = 4 << 10

	dummyBlocks := make([]blocks.Block, numBlocks)

	{
		// scope dummyRand to prevent its unsafe concurrent use below
		dummyRand := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := range dummyBlocks {
			dummy := make([]byte, 32)
			if _, err := io.ReadFull(dummyRand, dummy); err != nil {
				b.Fatal(err)
			}
			dummyBlocks[i] = blocks.NewBlock(dummy)
		}
	}

	// Each test begins with half the blocks present in the cache.
	// This allows test cases to have both hits and misses,
	// regardless of whether or not they do Puts.
	putHalfBlocks := func(c *tqcache) {
		for i, block := range dummyBlocks {
			if i%2 == 0 {
				if err := c.Put(bg, block); err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	// We always mix just two operations at a time.
	const numOps = 2
	testOps := []struct {
		name string
		ops  [numOps]func(*tqcache, blocks.Block)
	}{
		{"PutDelete", [...]func(*tqcache, blocks.Block){
			func(c *tqcache, block blocks.Block) {
				c.Put(bg, block)
			},
			func(c *tqcache, block blocks.Block) {
				c.DeleteBlock(bg, block.Cid())
			},
		}},
		{"GetDelete", [...]func(*tqcache, blocks.Block){
			func(c *tqcache, block blocks.Block) {
				c.Get(bg, block.Cid())
			},
			func(c *tqcache, block blocks.Block) {
				c.DeleteBlock(bg, block.Cid())
			},
		}},
		{"GetPut", [...]func(*tqcache, blocks.Block){
			func(c *tqcache, block blocks.Block) {
				c.Get(bg, block.Cid())
			},
			func(c *tqcache, block blocks.Block) {
				c.Put(bg, block)
			},
		}},
	}

	for _, test := range testOps {
		test := test // prevent reuse of the range var
		b.Run(test.name, func(b *testing.B) {
			c, _, _ := createStores(b)
			putHalfBlocks(c)
			var opCounts [numOps]uint64

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					n := rnd.Int63()
					blockIdx := n % numBlocks         // lower bits decide the block
					opIdx := (n / numBlocks) % numOps // higher bits decide what operation

					block := dummyBlocks[blockIdx]
					op := test.ops[opIdx]
					op(c, block)

					atomic.AddUint64(&opCounts[opIdx], 1)
				}
			})

			// We expect each op to fire roughly an equal amount of times.
			// Error otherwise, as that likely means the logic is wrong.
			var minIdx, maxIdx int
			var minCount, maxCount uint64
			for opIdx, count := range opCounts {
				if minCount == 0 || count < minCount {
					minIdx = opIdx
					minCount = count
				}
				if maxCount == 0 || count > maxCount {
					maxIdx = opIdx
					maxCount = count
				}
			}
			// Skip this check if we ran few times, to avoid false positives.
			if maxCount > 100 {
				ratio := float64(maxCount) / float64(minCount)
				if maxRatio := 2.0; ratio > maxRatio {
					b.Fatalf("op %d ran %fx as many times as %d", maxIdx, ratio, minIdx)
				}
			}
		})
	}
}
