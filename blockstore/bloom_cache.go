package blockstore

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	bloom "github.com/ipfs/bbloom"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	metrics "github.com/ipfs/go-metrics-interface"
)

// bloomCached returns a Blockstore that caches Has requests using a Bloom
// filter. bloomSize is size of bloom filter in bytes. hashCount specifies the
// number of hashing functions in the bloom filter (usually known as k).
func bloomCached(ctx context.Context, bs Blockstore, bloomSize, hashCount int) (*bloomcache, error) {
	bl, err := bloom.New(float64(bloomSize), float64(hashCount))
	if err != nil {
		return nil, err
	}
	bc := &bloomcache{
		blockstore: bs,
		bloom:      bl,
		hits: metrics.NewCtx(ctx, "bloom.hits_total",
			"Number of cache hits in bloom cache").Counter(),
		total: metrics.NewCtx(ctx, "bloom_total",
			"Total number of requests to bloom cache").Counter(),
		buildChan: make(chan struct{}),
	}
	if v, ok := bs.(Viewer); ok {
		bc.viewer = v
	}
	go func() {
		err := bc.build(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				logger.Warn("Cache rebuild closed by context finishing: ", err)
			default:
				logger.Error(err)
			}
			return
		}
		if metrics.Active() {
			fill := metrics.NewCtx(ctx, "bloom_fill_ratio",
				"Ratio of bloom filter fullnes, (updated once a minute)").Gauge()

			t := time.NewTicker(1 * time.Minute)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					fill.Set(bc.bloom.FillRatioTS())
				}
			}
		}
	}()
	return bc, nil
}

type bloomcache struct {
	active int32

	bloom    *bloom.Bloom
	buildErr error

	buildChan  chan struct{}
	blockstore Blockstore
	viewer     Viewer

	// Statistics
	hits  metrics.Counter
	total metrics.Counter
}

var (
	_ Blockstore           = (*bloomcache)(nil)
	_ Viewer               = (*bloomcache)(nil)
	_ allKeysChanWithErrer = (*bloomcache)(nil)
)

func (b *bloomcache) BloomActive() bool {
	return atomic.LoadInt32(&b.active) != 0
}

func (b *bloomcache) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.buildChan:
		return b.buildErr
	}
}

func (b *bloomcache) build(ctx context.Context) error {
	logger.Debug("begin building bloomcache")
	start := time.Now()
	defer func() {
		logger.Debugf("bloomcache build finished in %s", time.Since(start))
	}()
	defer close(b.buildChan)

	ch, errFn, err := allKeysChanWithErrFor(ctx, b.blockstore)
	if err != nil {
		b.buildErr = fmt.Errorf("AllKeysChan failed in bloomcache build with: %w", err)
		return b.buildErr
	}
	for {
		select {
		case key, ok := <-ch:
			if !ok {
				// A closed channel alone does not prove the enumeration was
				// complete: it could have been truncated by a mid-iteration
				// error or a cancelled context. Trust the filter only if every
				// key was delivered, otherwise the "not in bloom" answer would
				// be a false negative for blocks that exist but were never
				// indexed.
				//
				// If the wrapped store does not implement allKeysChanWithErrer,
				// errFn is a no-op returning nil (see allKeysChanWithErrFor) and
				// the filter is treated as complete, preserving the pre-existing
				// best-effort behavior for such stores.
				if err := errFn(); err != nil {
					b.buildErr = fmt.Errorf("bloomcache build incomplete, not activating filter: %w", err)
					return b.buildErr
				}
				atomic.StoreInt32(&b.active, 1)
				return nil
			}
			b.bloom.AddTS(key.Hash()) // Use binary key, the more compact the better
		case <-ctx.Done():
			b.buildErr = ctx.Err()
			return b.buildErr
		}
	}
}

// allKeysChanWithErr forwards the error-reporting enumeration to the wrapped
// blockstore, so the completeness signal survives the cache stack.
func (b *bloomcache) allKeysChanWithErr(ctx context.Context) (<-chan cid.Cid, func() error, error) {
	return allKeysChanWithErrFor(ctx, b.blockstore)
}

func (b *bloomcache) DeleteBlock(ctx context.Context, k cid.Cid) error {
	if has, ok := b.hasCached(k); ok && !has {
		return nil
	}

	return b.blockstore.DeleteBlock(ctx, k)
}

// if ok == false has is inconclusive
// if ok == true then has respons to question: is it contained
func (b *bloomcache) hasCached(k cid.Cid) (has bool, ok bool) {
	b.total.Inc()
	if !k.Defined() {
		logger.Error("undefined in bloom cache")
		// Return cache invalid so call to blockstore
		// in case of invalid key is forwarded deeper
		return false, false
	}
	if b.BloomActive() {
		blr := b.bloom.HasTS(k.Hash())
		if !blr { // not contained in bloom is only conclusive answer bloom gives
			b.hits.Inc()
			return false, true
		}
	}
	return false, false
}

func (b *bloomcache) Has(ctx context.Context, k cid.Cid) (bool, error) {
	if has, ok := b.hasCached(k); ok {
		return has, nil
	}

	return b.blockstore.Has(ctx, k)
}

func (b *bloomcache) GetSize(ctx context.Context, k cid.Cid) (int, error) {
	if has, ok := b.hasCached(k); ok && !has {
		return -1, ipld.ErrNotFound{Cid: k}
	}

	return b.blockstore.GetSize(ctx, k)
}

func (b *bloomcache) View(ctx context.Context, k cid.Cid, callback func([]byte) error) error {
	if b.viewer == nil {
		blk, err := b.Get(ctx, k)
		if err != nil {
			return err
		}
		return callback(blk.RawData())
	}

	if has, ok := b.hasCached(k); ok && !has {
		return ipld.ErrNotFound{Cid: k}
	}
	return b.viewer.View(ctx, k, callback)
}

func (b *bloomcache) Get(ctx context.Context, k cid.Cid) (blocks.Block, error) {
	if has, ok := b.hasCached(k); ok && !has {
		return nil, ipld.ErrNotFound{Cid: k}
	}

	return b.blockstore.Get(ctx, k)
}

func (b *bloomcache) Put(ctx context.Context, bl blocks.Block) error {
	// See comment in PutMany
	err := b.blockstore.Put(ctx, bl)
	if err == nil {
		b.bloom.AddTS(bl.Cid().Hash())
	}
	return err
}

func (b *bloomcache) PutMany(ctx context.Context, bs []blocks.Block) error {
	// bloom cache gives only conclusive resulty if key is not contained
	// to reduce number of puts we need conclusive information if block is contained
	// this means that PutMany can't be improved with bloom cache so we just
	// just do a passthrough.
	err := b.blockstore.PutMany(ctx, bs)
	if err != nil {
		return err
	}
	for _, bl := range bs {
		b.bloom.AddTS(bl.Cid().Hash())
	}
	return nil
}

func (b *bloomcache) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return b.blockstore.AllKeysChan(ctx)
}

func (b *bloomcache) GCLock(ctx context.Context) Unlocker {
	return b.blockstore.(GCBlockstore).GCLock(ctx)
}

func (b *bloomcache) PinLock(ctx context.Context) Unlocker {
	return b.blockstore.(GCBlockstore).PinLock(ctx)
}

func (b *bloomcache) GCRequested(ctx context.Context) bool {
	return b.blockstore.(GCBlockstore).GCRequested(ctx)
}
