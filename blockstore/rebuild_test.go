package blockstore

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	syncds "github.com/ipfs/go-datastore/sync"
)

// toggleErrDS injects a mid-iteration enumeration error only while fail is set,
// so a test can fail an initial build and then let a Rebuild succeed.
type toggleErrDS struct {
	ds.Batching
	after int
	fail  atomic.Bool
}

func (d *toggleErrDS) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	res, err := d.Batching.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	if !d.fail.Load() {
		return res, nil
	}
	return dsq.ResultsWithContext(q, func(ctx context.Context, out chan<- dsq.Result) {
		defer res.Close()
		n := 0
		for {
			r, ok := res.NextSync()
			if !ok {
				return
			}
			if r.Error != nil {
				out <- r
				return
			}
			if n >= d.after {
				out <- dsq.Result{Error: errInjected}
				return
			}
			select {
			case <-ctx.Done():
				return
			case out <- r:
			}
			n++
		}
	}), nil
}

// hookDS invokes hook once, after emitting `at` entries of an enumeration. The
// underlying MapDatastore materializes query results eagerly, so a block
// written by the hook is NOT seen by the in-flight enumeration — it can only
// reach the new filter through the concurrent write path.
type hookDS struct {
	ds.Batching
	at   int
	hook func()
}

func (d *hookDS) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	res, err := d.Batching.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return dsq.ResultsWithContext(q, func(ctx context.Context, out chan<- dsq.Result) {
		defer res.Close()
		n := 0
		for {
			r, ok := res.NextSync()
			if !ok {
				return
			}
			if n == d.at && d.hook != nil {
				d.hook()
			}
			select {
			case <-ctx.Done():
				return
			case out <- r:
			}
			n++
		}
	}), nil
}

func TestRebuildActivatesAfterFailedBuild(t *testing.T) {
	under := syncds.MutexWrap(ds.NewMapDatastore())
	tds := &toggleErrDS{Batching: under, after: 10}
	tds.fail.Store(true)
	bs := NewBlockstore(tds)

	const total = 300
	keys := make([]cid.Cid, 0, total)
	for i := range total {
		b := blocks.NewBlock(fmt.Appendf(nil, "data %d", i))
		if err := bs.Put(bg, b); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, b.Cid())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cachedbs, err := testBloomCached(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}

	// The initial build fails: filter inactive, but reads remain correct.
	if err := cachedbs.Wait(ctx); err == nil {
		t.Fatal("expected initial build to fail")
	}
	if cachedbs.BloomActive() {
		t.Fatal("filter must be inactive after a failed build")
	}
	for _, k := range keys {
		if has, err := cachedbs.Has(bg, k); err != nil || !has {
			t.Fatalf("present block reported missing while inactive (has=%v err=%v)", has, err)
		}
	}

	// Fix the datastore and rebuild.
	tds.fail.Store(false)
	if err := cachedbs.Rebuild(ctx); err != nil {
		t.Fatalf("rebuild failed: %v", err)
	}
	if !cachedbs.BloomActive() {
		t.Fatal("filter must be active after a successful rebuild")
	}
	// Wait reflects only the initial build and is intentionally not updated by
	// Rebuild; callers observe a rebuild via its return value and BloomActive.
	if err := cachedbs.Wait(ctx); err == nil {
		t.Fatal("Wait should still report the initial build error after a Rebuild")
	}
	for _, k := range keys {
		if has, err := cachedbs.Has(bg, k); err != nil || !has {
			t.Fatalf("present block reported missing after rebuild (has=%v err=%v)", has, err)
		}
	}
	absent := blocks.NewBlock([]byte("absent-after-rebuild")).Cid()
	if has, err := cachedbs.Has(bg, absent); err != nil || has {
		t.Fatalf("absent block reported present after rebuild (has=%v err=%v)", has, err)
	}
}

func TestRebuildFailureLeavesCorrectPassThrough(t *testing.T) {
	under := syncds.MutexWrap(ds.NewMapDatastore())
	tds := &toggleErrDS{Batching: under, after: 10} // fail starts false: clean build
	bs := NewBlockstore(tds)

	const total = 200
	keys := make([]cid.Cid, 0, total)
	for i := range total {
		b := blocks.NewBlock(fmt.Appendf(nil, "data %d", i))
		if err := bs.Put(bg, b); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, b.Cid())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cachedbs, err := testBloomCached(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}
	if err := cachedbs.Wait(ctx); err != nil {
		t.Fatalf("initial build failed: %v", err)
	}
	if !cachedbs.BloomActive() {
		t.Fatal("filter should be active after a clean build")
	}

	// A rebuild that fails leaves the filter inactive (not active on partial
	// data), and reads stay correct via pass-through.
	tds.fail.Store(true)
	if err := cachedbs.Rebuild(ctx); err == nil {
		t.Fatal("expected rebuild to fail")
	}
	if cachedbs.BloomActive() {
		t.Fatal("filter must be inactive after a failed rebuild")
	}
	for _, k := range keys {
		if has, err := cachedbs.Has(bg, k); err != nil || !has {
			t.Fatalf("present block reported missing after failed rebuild (has=%v err=%v)", has, err)
		}
	}
	absent := blocks.NewBlock([]byte("absent-after-failed-rebuild")).Cid()
	if has, err := cachedbs.Has(bg, absent); err != nil || has {
		t.Fatalf("absent block reported present after failed rebuild (has=%v err=%v)", has, err)
	}
}

func TestRebuildContextCancelledIsNonDestructive(t *testing.T) {
	under, _ := newBlockStoreWithKeys(t, nil, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cachedbs, err := testBloomCached(ctx, under)
	if err != nil {
		t.Fatal(err)
	}
	if err := cachedbs.Wait(ctx); err != nil {
		t.Fatalf("initial build failed: %v", err)
	}

	cctx, ccancel := context.WithCancel(context.Background())
	ccancel() // already cancelled
	if err := cachedbs.Rebuild(cctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
	// An immediately-cancelled rebuild bails before touching the filter.
	if !cachedbs.BloomActive() {
		t.Fatal("an immediately-cancelled rebuild must not deactivate the filter")
	}
}

func TestRebuildCapturesConcurrentPut(t *testing.T) {
	under := syncds.MutexWrap(ds.NewMapDatastore())
	hds := &hookDS{Batching: under, at: 25}
	bs := NewBlockstore(hds)

	const total = 50
	for i := range total {
		if err := bs.Put(bg, blocks.NewBlock(fmt.Appendf(nil, "data %d", i))); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cachedbs, err := testBloomCached(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}
	if err := cachedbs.Wait(ctx); err != nil {
		t.Fatalf("initial build failed: %v", err)
	}

	// Arm a hook (after the initial build) that writes a new block partway
	// through the rebuild enumeration. The eager query snapshot will not see
	// it, so it can only land in the new filter via the concurrent write path.
	concurrent := blocks.NewBlock([]byte("written-during-rebuild"))
	hds.hook = func() {
		if err := cachedbs.Put(bg, concurrent); err != nil {
			t.Errorf("concurrent put failed: %v", err)
		}
	}

	if err := cachedbs.Rebuild(ctx); err != nil {
		t.Fatalf("rebuild failed: %v", err)
	}
	if !cachedbs.BloomActive() {
		t.Fatal("filter must be active after rebuild")
	}
	if has, err := cachedbs.Has(bg, concurrent.Cid()); err != nil || !has {
		t.Fatalf("block written during rebuild reported missing (has=%v err=%v)", has, err)
	}
}

func TestBloomCacheStatusRebuildReachable(t *testing.T) {
	under, _ := newBlockStoreWithKeys(t, nil, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cbs, err := CachedBlockstore(ctx, under, DefaultCacheOpts())
	if err != nil {
		t.Fatal(err)
	}
	s, ok := cbs.(BloomCacheStatus)
	if !ok {
		t.Fatal("CachedBlockstore result does not implement BloomCacheStatus")
	}
	if err := s.Wait(ctx); err != nil {
		t.Fatalf("initial build via BloomCacheStatus: %v", err)
	}
	if err := s.Rebuild(ctx); err != nil {
		t.Fatalf("rebuild via BloomCacheStatus: %v", err)
	}
	if !s.BloomActive() {
		t.Fatal("expected filter active after rebuild")
	}
}
