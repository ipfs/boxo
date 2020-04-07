package blockstore

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	ds_sync "github.com/ipfs/go-datastore/sync"
	u "github.com/ipfs/go-ipfs-util"
	ipld "github.com/ipfs/go-ipld-format"
)

func TestGetWhenKeyNotPresent(t *testing.T) {
	bs := NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
	c := cid.NewCidV0(u.Hash([]byte("stuff")))
	bl, err := bs.Get(bg, c)

	if bl != nil {
		t.Error("nil block expected")
	}
	if err == nil {
		t.Error("error expected, got nil")
	}
}

func TestGetWhenKeyIsNil(t *testing.T) {
	bs := NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
	_, err := bs.Get(bg, cid.Cid{})
	if !ipld.IsNotFound(err) {
		t.Fail()
	}
}

func TestPutThenGetBlock(t *testing.T) {
	bs := NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
	block := blocks.NewBlock([]byte("some data"))

	err := bs.Put(bg, block)
	if err != nil {
		t.Fatal(err)
	}

	blockFromBlockstore, err := bs.Get(bg, block.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(block.RawData(), blockFromBlockstore.RawData()) {
		t.Fail()
	}
}

func TestCidv0v1(t *testing.T) {
	bs := NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
	block := blocks.NewBlock([]byte("some data"))

	err := bs.Put(bg, block)
	if err != nil {
		t.Fatal(err)
	}

	blockFromBlockstore, err := bs.Get(bg, cid.NewCidV1(cid.DagProtobuf, block.Cid().Hash()))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(block.RawData(), blockFromBlockstore.RawData()) {
		t.Fail()
	}
}

func TestPutThenGetSizeBlock(t *testing.T) {
	bs := NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
	block := blocks.NewBlock([]byte("some data"))
	missingBlock := blocks.NewBlock([]byte("missingBlock"))
	emptyBlock := blocks.NewBlock([]byte{})

	err := bs.Put(bg, block)
	if err != nil {
		t.Fatal(err)
	}

	blockSize, err := bs.GetSize(bg, block.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if len(block.RawData()) != blockSize {
		t.Fail()
	}

	err = bs.Put(bg, emptyBlock)
	if err != nil {
		t.Fatal(err)
	}

	if blockSize, err := bs.GetSize(bg, emptyBlock.Cid()); blockSize != 0 || err != nil {
		t.Fatal(err)
	}

	if blockSize, err := bs.GetSize(bg, missingBlock.Cid()); blockSize != -1 || err == nil {
		t.Fatal("getsize returned invalid result")
	}
}

type countHasDS struct {
	ds.Datastore
	hasCount int
}

func (ds *countHasDS) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	ds.hasCount += 1
	return ds.Datastore.Has(ctx, key)
}

func TestPutUsesHas(t *testing.T) {
	// Some datastores rely on the implementation detail that Put checks Has
	// first, to avoid overriding existing objects' metadata. This test ensures
	// that Blockstore continues to behave this way.
	// Please ping https://github.com/ipfs/go-ipfs-blockstore/pull/47 if this
	// behavior is being removed.
	ds := &countHasDS{
		Datastore: ds.NewMapDatastore(),
	}
	bs := NewBlockstore(ds_sync.MutexWrap(ds))
	bl := blocks.NewBlock([]byte("some data"))
	if err := bs.Put(bg, bl); err != nil {
		t.Fatal(err)
	}
	if err := bs.Put(bg, bl); err != nil {
		t.Fatal(err)
	}
	if ds.hasCount != 2 {
		t.Errorf("Blockstore did not call Has before attempting Put, this breaks compatibility")
	}
}

func TestHashOnRead(t *testing.T) {
	orginalDebug := u.Debug
	defer (func() {
		u.Debug = orginalDebug
	})()
	u.Debug = false

	bs := NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
	bl := blocks.NewBlock([]byte("some data"))
	blBad, err := blocks.NewBlockWithCid([]byte("some other data"), bl.Cid())
	if err != nil {
		t.Fatal("debug is off, still got an error")
	}
	bl2 := blocks.NewBlock([]byte("some other data"))
	bs.Put(bg, blBad)
	bs.Put(bg, bl2)
	bs.HashOnRead(true)

	if _, err := bs.Get(bg, bl.Cid()); err != ErrHashMismatch {
		t.Fatalf("expected '%v' got '%v'\n", ErrHashMismatch, err)
	}

	if b, err := bs.Get(bg, bl2.Cid()); err != nil || b.String() != bl2.String() {
		t.Fatal("got wrong blocks")
	}
}

func newBlockStoreWithKeys(t *testing.T, d ds.Datastore, N int) (Blockstore, []cid.Cid) {
	if d == nil {
		d = ds.NewMapDatastore()
	}
	bs := NewBlockstore(ds_sync.MutexWrap(d))

	keys := make([]cid.Cid, N)
	for i := 0; i < N; i++ {
		block := blocks.NewBlock([]byte(fmt.Sprintf("some data %d", i)))
		err := bs.Put(bg, block)
		if err != nil {
			t.Fatal(err)
		}
		keys[i] = block.Cid()
	}
	return bs, keys
}

func collect(ch <-chan cid.Cid) []cid.Cid {
	var keys []cid.Cid
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

func TestAllKeysSimple(t *testing.T) {
	bs, keys := newBlockStoreWithKeys(t, nil, 100)

	ctx := context.Background()
	ch, err := bs.AllKeysChan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys2 := collect(ch)

	// for _, k2 := range keys2 {
	// 	t.Log("found ", k2.B58String())
	// }

	expectMatches(t, keys, keys2)
}

func TestAllKeysRespectsContext(t *testing.T) {
	N := 100

	d := &queryTestDS{ds: ds.NewMapDatastore()}
	bs, _ := newBlockStoreWithKeys(t, d, N)

	started := make(chan struct{}, 1)
	done := make(chan struct{}, 1)
	errors := make(chan error, 100)

	getKeys := func(ctx context.Context) {
		started <- struct{}{}
		ch, err := bs.AllKeysChan(ctx) // once without cancelling
		if err != nil {
			errors <- err
		}
		_ = collect(ch)
		done <- struct{}{}
		errors <- nil // a nil one to signal break
	}

	var results dsq.Results
	var resultsmu = make(chan struct{})
	resultChan := make(chan dsq.Result)
	d.SetFunc(func(q dsq.Query) (dsq.Results, error) {
		results = dsq.ResultsWithChan(q, resultChan)
		resultsmu <- struct{}{}
		return results, nil
	})

	go getKeys(context.Background())

	// make sure it's waiting.
	<-started
	<-resultsmu
	select {
	case <-done:
		t.Fatal("sync is wrong")
	case <-results.Process().Closing():
		t.Fatal("should not be closing")
	case <-results.Process().Closed():
		t.Fatal("should not be closed")
	default:
	}

	e := dsq.Entry{Key: BlockPrefix.ChildString("foo").String()}
	resultChan <- dsq.Result{Entry: e} // let it go.
	close(resultChan)
	<-done                       // should be done now.
	<-results.Process().Closed() // should be closed now

	// print any errors
	for err := range errors {
		if err == nil {
			break
		}
		t.Error(err)
	}

}

func expectMatches(t *testing.T, expect, actual []cid.Cid) {
	t.Helper()

	if len(expect) != len(actual) {
		t.Errorf("expect and actual differ: %d != %d", len(expect), len(actual))
	}

	actualSet := make(map[string]bool, len(actual))
	for _, k := range actual {
		actualSet[string(k.Hash())] = true
	}

	for _, ek := range expect {
		if !actualSet[string(ek.Hash())] {
			t.Error("expected key not found: ", ek)
		}
	}
}

type queryTestDS struct {
	cb func(q dsq.Query) (dsq.Results, error)
	ds ds.Datastore
}

func (c *queryTestDS) SetFunc(f func(dsq.Query) (dsq.Results, error)) { c.cb = f }

func (c *queryTestDS) Put(ctx context.Context, key ds.Key, value []byte) (err error) {
	return c.ds.Put(ctx, key, value)
}

func (c *queryTestDS) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	return c.ds.Get(ctx, key)
}

func (c *queryTestDS) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	return c.ds.Has(ctx, key)
}

func (c *queryTestDS) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	return c.ds.GetSize(ctx, key)
}

func (c *queryTestDS) Delete(ctx context.Context, key ds.Key) (err error) {
	return c.ds.Delete(ctx, key)
}

func (c *queryTestDS) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	if c.cb != nil {
		return c.cb(q)
	}
	return c.ds.Query(ctx, q)
}

func (c *queryTestDS) Sync(ctx context.Context, key ds.Key) error {
	return c.ds.Sync(ctx, key)
}

func (c *queryTestDS) Batch(_ context.Context) (ds.Batch, error) {
	return ds.NewBasicBatch(c), nil
}
func (c *queryTestDS) Close() error {
	return nil
}
