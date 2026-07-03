package blockstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	syncds "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
)

var errInjected = errors.New("simulated mid-iteration datastore error")

// errAfterDS wraps a datastore and injects a mid-iteration query error after
// `after` entries have been streamed, simulating an I/O or iterator error
// partway through enumeration (a documented possibility: see dsq.Result.Error).
type errAfterDS struct {
	ds.Batching
	after int
}

func (d *errAfterDS) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
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

// cleanChanErrFnBS is a Blockstore whose allKeysChanWithErr returns a channel
// that closes normally, yet reports a non-nil enumeration error. It isolates
// the core invariant: the Bloom build must trust the reported error, not the
// fact that the channel closed.
type cleanChanErrFnBS struct {
	Blockstore
	enumErr error
}

func (f *cleanChanErrFnBS) allKeysChanWithErr(ctx context.Context) (<-chan cid.Cid, func() error, error) {
	ch, err := f.AllKeysChan(ctx)
	if err != nil {
		return nil, nil, err
	}
	return ch, func() error { return f.enumErr }, nil
}

func TestAllKeysChanWithErrClean(t *testing.T) {
	bs, keys := newBlockStoreWithKeys(t, nil, 100)

	e := bs.(allKeysChanWithErrer)
	ch, errFn, err := e.allKeysChanWithErr(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got := collect(ch)
	if err := errFn(); err != nil {
		t.Fatalf("errFn returned error on clean enumeration: %v", err)
	}
	expectMatches(t, keys, got)
}

func TestAllKeysChanWithErrMidIteration(t *testing.T) {
	const total = 100
	const after = 10

	under := syncds.MutexWrap(ds.NewMapDatastore())
	bs := NewBlockstore(&errAfterDS{Batching: under, after: after})
	for i := range total {
		if err := bs.Put(t.Context(), blocks.NewBlock(fmt.Appendf(nil, "data %d", i))); err != nil {
			t.Fatal(err)
		}
	}

	e := bs.(allKeysChanWithErrer)
	ch, errFn, err := e.allKeysChanWithErr(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got := collect(ch)

	iterErr := errFn()
	if iterErr == nil {
		t.Fatal("expected errFn to report the mid-iteration error, got nil")
	}
	if !errors.Is(iterErr, errInjected) {
		t.Fatalf("expected wrapped errInjected, got: %v", iterErr)
	}
	if len(got) >= total {
		t.Fatalf("expected partial enumeration (< %d keys), got %d", total, len(got))
	}
}

func TestAllKeysChanWithErrContextCancel(t *testing.T) {
	// More keys than the channel buffer so the producer is forced to block on
	// a send once we stop draining, making the cancellation deterministic.
	n := 2*dsq.KeysOnlyBufSize + 10
	bs, _ := newBlockStoreWithKeys(t, nil, n)

	e := bs.(allKeysChanWithErrer)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	ch, errFn, err := e.allKeysChanWithErr(ctx)
	if err != nil {
		t.Fatal(err)
	}

	<-ch // ensure enumeration started, then stop draining
	cancel()

	if iterErr := errFn(); !errors.Is(iterErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", iterErr)
	}
}

func TestBloomBuildDoesNotActivateOnIncompleteEnumeration(t *testing.T) {
	under, keys := newBlockStoreWithKeys(t, nil, 100)
	enumErr := errors.New("incomplete enumeration")
	fake := &cleanChanErrFnBS{Blockstore: under, enumErr: enumErr}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	cachedbs, err := testBloomCached(ctx, fake)
	if err != nil {
		t.Fatal(err)
	}

	waitErr := cachedbs.Wait(ctx)
	if waitErr == nil {
		t.Fatal("expected Wait to return the build error, got nil")
	}
	if !errors.Is(waitErr, enumErr) {
		t.Fatalf("expected wrapped enumErr, got: %v", waitErr)
	}
	if cachedbs.BloomActive() {
		t.Fatal("bloom must not be active after an incomplete build")
	}

	// Despite the failed build, the cache must answer correctly by falling
	// through to the underlying store.
	for _, k := range keys {
		has, err := cachedbs.Has(t.Context(), k)
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Fatalf("present block %s reported missing after incomplete build", k)
		}
	}
	absent := blocks.NewBlock([]byte("definitely-absent")).Cid()
	if has, err := cachedbs.Has(t.Context(), absent); err != nil || has {
		t.Fatalf("absent block reported present (has=%v err=%v)", has, err)
	}

	// Get / GetSize must also pass through to the underlying store, and
	// DeleteBlock must actually reach it (not be skipped as a no-op).
	if blk, err := cachedbs.Get(t.Context(), keys[0]); err != nil || blk == nil {
		t.Fatalf("Get of present block failed under inactive bloom (blk=%v err=%v)", blk, err)
	}
	if sz, err := cachedbs.GetSize(t.Context(), keys[0]); err != nil || sz < 0 {
		t.Fatalf("GetSize of present block failed under inactive bloom (sz=%d err=%v)", sz, err)
	}
	delKey := keys[len(keys)-1]
	if err := cachedbs.DeleteBlock(t.Context(), delKey); err != nil {
		t.Fatal(err)
	}
	if has, err := cachedbs.Has(t.Context(), delKey); err != nil || has {
		t.Fatalf("DeleteBlock under inactive bloom did not delete (has=%v err=%v)", has, err)
	}
}

func TestBloomBuildMidIterationErrorEndToEnd(t *testing.T) {
	const total = 1000
	const after = 100

	under := syncds.MutexWrap(ds.NewMapDatastore())
	bs := NewBlockstore(&errAfterDS{Batching: under, after: after})
	keys := make([]cid.Cid, 0, total)
	for i := range total {
		b := blocks.NewBlock(fmt.Appendf(nil, "data %d", i))
		if err := bs.Put(t.Context(), b); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, b.Cid())
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	cachedbs, err := testBloomCached(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}

	waitErr := cachedbs.Wait(ctx)
	if waitErr == nil {
		t.Fatal("expected Wait to return the incomplete-build error, got nil")
	}
	if !errors.Is(waitErr, errInjected) {
		t.Fatalf("expected wrapped errInjected, got: %v", waitErr)
	}
	if cachedbs.BloomActive() {
		t.Fatal("bloom must not be active after a mid-iteration enumeration error")
	}

	// All present blocks must still be reported present (pass-through).
	for _, k := range keys {
		has, err := cachedbs.Has(t.Context(), k)
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Fatalf("present block %s reported missing", k)
		}
	}
}

func TestBloomBuildCleanPathActivates(t *testing.T) {
	under, keys := newBlockStoreWithKeys(t, nil, 200)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	cachedbs, err := testBloomCached(ctx, under)
	if err != nil {
		t.Fatal(err)
	}

	if err := cachedbs.Wait(ctx); err != nil {
		t.Fatalf("clean build returned error: %v", err)
	}
	if !cachedbs.BloomActive() {
		t.Fatal("bloom should be active after a clean build")
	}

	for _, k := range keys {
		if has, err := cachedbs.Has(t.Context(), k); err != nil || !has {
			t.Fatalf("present block reported missing (has=%v err=%v)", has, err)
		}
	}
	absent := blocks.NewBlock([]byte("nope-not-here")).Cid()
	if has, err := cachedbs.Has(t.Context(), absent); err != nil || has {
		t.Fatalf("absent block reported present (has=%v err=%v)", has, err)
	}
}

func TestAllKeysChanWithErrForwardsThroughCacheStack(t *testing.T) {
	const total = 100
	const after = 10

	under := syncds.MutexWrap(ds.NewMapDatastore())
	bs := NewBlockstore(&errAfterDS{Batching: under, after: after})
	for i := range total {
		if err := bs.Put(t.Context(), blocks.NewBlock(fmt.Appendf(nil, "data %d", i))); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	// Default opts wrap in BOTH a two-queue cache and a Bloom filter, so the
	// capability must traverse bloomcache -> tqcache -> base blockstore.
	cbs, err := CachedBlockstore(ctx, bs, DefaultCacheOpts())
	if err != nil {
		t.Fatal(err)
	}

	e, ok := cbs.(allKeysChanWithErrer)
	if !ok {
		t.Fatal("CachedBlockstore result does not implement allKeysChanWithErrer")
	}
	ch, errFn, err := e.allKeysChanWithErr(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = collect(ch)
	if iterErr := errFn(); !errors.Is(iterErr, errInjected) {
		t.Fatalf("expected errInjected to propagate through the cache stack, got: %v", iterErr)
	}
}

// legacyBS implements only the Blockstore interface (no allKeysChanWithErrer),
// to exercise the best-effort fallback in allKeysChanWithErrFor. It must NOT
// embed *blockstore, or the capability method would be promoted and the
// fallback branch would never run.
type legacyBS struct{ inner Blockstore }

func (b *legacyBS) DeleteBlock(ctx context.Context, k cid.Cid) error {
	return b.inner.DeleteBlock(ctx, k)
}
func (b *legacyBS) Has(ctx context.Context, k cid.Cid) (bool, error) { return b.inner.Has(ctx, k) }
func (b *legacyBS) Get(ctx context.Context, k cid.Cid) (blocks.Block, error) {
	return b.inner.Get(ctx, k)
}

func (b *legacyBS) GetSize(ctx context.Context, k cid.Cid) (int, error) {
	return b.inner.GetSize(ctx, k)
}
func (b *legacyBS) Put(ctx context.Context, bl blocks.Block) error { return b.inner.Put(ctx, bl) }
func (b *legacyBS) PutMany(ctx context.Context, bls []blocks.Block) error {
	return b.inner.PutMany(ctx, bls)
}

func (b *legacyBS) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return b.inner.AllKeysChan(ctx)
}

func TestAllKeysChanWithErrFallbackForLegacyBlockstore(t *testing.T) {
	under, keys := newBlockStoreWithKeys(t, nil, 100)
	legacy := &legacyBS{inner: under}

	// Guard the premise: legacyBS must NOT advertise the capability, else the
	// fallback branch is not exercised.
	if _, ok := Blockstore(legacy).(allKeysChanWithErrer); ok {
		t.Fatal("legacyBS unexpectedly implements allKeysChanWithErrer")
	}

	// The fallback yields a no-op errFn and a full enumeration.
	ch, errFn, err := allKeysChanWithErrFor(t.Context(), legacy)
	if err != nil {
		t.Fatal(err)
	}
	got := collect(ch)
	if err := errFn(); err != nil {
		t.Fatalf("fallback errFn must return nil, got: %v", err)
	}
	expectMatches(t, keys, got)

	// Over a fully-enumerable legacy store, the bloom still activates.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	cachedbs, err := testBloomCached(ctx, legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := cachedbs.Wait(ctx); err != nil {
		t.Fatalf("clean build over legacy store returned error: %v", err)
	}
	if !cachedbs.BloomActive() {
		t.Fatal("bloom should activate over a fully-enumerable legacy store")
	}
	for _, k := range keys {
		if has, err := cachedbs.Has(t.Context(), k); err != nil || !has {
			t.Fatalf("present block reported missing (has=%v err=%v)", has, err)
		}
	}
}

func TestAllKeysChanWithErrForwardingWrappers(t *testing.T) {
	const total = 100
	const after = 10

	newBaseWithErr := func() Blockstore {
		under := syncds.MutexWrap(ds.NewMapDatastore())
		bs := NewBlockstore(&errAfterDS{Batching: under, after: after})
		for i := range total {
			if err := bs.Put(t.Context(), blocks.NewBlock(fmt.Appendf(nil, "data %d", i))); err != nil {
				t.Fatal(err)
			}
		}
		return bs
	}

	wrappers := map[string]func(Blockstore) Blockstore{
		"idstore":              func(bs Blockstore) Blockstore { return NewIdStore(bs) },
		"ValidatingBlockstore": func(bs Blockstore) Blockstore { return &ValidatingBlockstore{Blockstore: bs} },
	}

	for name, wrap := range wrappers {
		t.Run(name, func(t *testing.T) {
			w := wrap(newBaseWithErr())
			e, ok := w.(allKeysChanWithErrer)
			if !ok {
				t.Fatalf("%s does not implement allKeysChanWithErrer", name)
			}
			ch, errFn, err := e.allKeysChanWithErr(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			_ = collect(ch)
			if iterErr := errFn(); !errors.Is(iterErr, errInjected) {
				t.Fatalf("%s did not propagate enumeration error, got: %v", name, iterErr)
			}
		})
	}
}

func TestAllKeysChanWithErrSkipsUnparseableKey(t *testing.T) {
	raw := syncds.MutexWrap(ds.NewMapDatastore())
	bs := NewBlockstore(raw)

	const total = 50
	keys := make([]cid.Cid, 0, total)
	for i := range total {
		b := blocks.NewBlock(fmt.Appendf(nil, "data %d", i))
		if err := bs.Put(t.Context(), b); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, b.Cid())
	}

	// Inject a datastore key under the block prefix that is not valid base32,
	// so BinaryFromDsKey fails to parse it during enumeration.
	badKey := BlockPrefix.ChildString("this-is-not-base32")
	if err := raw.Put(t.Context(), badKey, []byte("garbage")); err != nil {
		t.Fatal(err)
	}

	e := bs.(allKeysChanWithErrer)
	ch, errFn, err := e.allKeysChanWithErr(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got := collect(ch)

	// The unparseable key is skipped, NOT treated as a truncating error: it
	// cannot correspond to a retrievable block, so it must not disable the
	// completeness signal.
	if err := errFn(); err != nil {
		t.Fatalf("unparseable key must not be reported as an enumeration error, got: %v", err)
	}
	expectMatches(t, keys, got)

	// The bloom filter still activates over such a store.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	cachedbs, err := testBloomCached(ctx, bs)
	if err != nil {
		t.Fatal(err)
	}
	if err := cachedbs.Wait(ctx); err != nil {
		t.Fatalf("build over store with an unparseable key returned error: %v", err)
	}
	if !cachedbs.BloomActive() {
		t.Fatal("bloom should activate despite an unparseable datastore key")
	}
}

// incompleteViewerBS reports an enumeration error (so the bloom build stays
// inactive) AND implements Viewer (so bloomcache wires up its viewer path),
// exercising bloomcache.View's pass-through gating while inactive.
type incompleteViewerBS struct {
	Blockstore
	enumErr error
}

func (f *incompleteViewerBS) allKeysChanWithErr(ctx context.Context) (<-chan cid.Cid, func() error, error) {
	ch, err := f.AllKeysChan(ctx)
	if err != nil {
		return nil, nil, err
	}
	return ch, func() error { return f.enumErr }, nil
}

func (f *incompleteViewerBS) View(ctx context.Context, k cid.Cid, callback func([]byte) error) error {
	blk, err := f.Get(ctx, k)
	if err != nil {
		return err
	}
	return callback(blk.RawData())
}

func TestBloomViewPassThroughWhenInactive(t *testing.T) {
	under, keys := newBlockStoreWithKeys(t, nil, 50)
	fake := &incompleteViewerBS{Blockstore: under, enumErr: errors.New("incomplete enumeration")}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	cachedbs, err := testBloomCached(ctx, fake)
	if err != nil {
		t.Fatal(err)
	}
	if err := cachedbs.Wait(ctx); err == nil {
		t.Fatal("expected incomplete-build error")
	}
	if cachedbs.BloomActive() {
		t.Fatal("bloom must be inactive")
	}

	// View passes through to the underlying viewer for a present block.
	var got []byte
	if err := cachedbs.View(t.Context(), keys[0], func(b []byte) error { got = append(got, b...); return nil }); err != nil {
		t.Fatalf("View of present block failed under inactive bloom: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("View returned empty data for a present block")
	}

	// View of an absent block returns not-found.
	absent := blocks.NewBlock([]byte("absent-view")).Cid()
	if err := cachedbs.View(t.Context(), absent, func(b []byte) error { return nil }); !ipld.IsNotFound(err) {
		t.Fatalf("View of absent block: expected not-found, got %v", err)
	}
}
