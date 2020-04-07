package blockservice

import (
	"context"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	butil "github.com/ipfs/go-ipfs-blocksutil"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipld "github.com/ipfs/go-ipld-format"
)

func TestWriteThroughWorks(t *testing.T) {
	bstore := &PutCountingBlockstore{
		blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore())),
		0,
	}
	bstore2 := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	exch := offline.Exchange(bstore2)
	bserv := NewWriteThrough(bstore, exch)
	bgen := butil.NewBlockGenerator()

	block := bgen.Next()

	t.Logf("PutCounter: %d", bstore.PutCounter)
	err := bserv.AddBlock(context.Background(), block)
	if err != nil {
		t.Fatal(err)
	}
	if bstore.PutCounter != 1 {
		t.Fatalf("expected just one Put call, have: %d", bstore.PutCounter)
	}

	err = bserv.AddBlock(context.Background(), block)
	if err != nil {
		t.Fatal(err)
	}
	if bstore.PutCounter != 2 {
		t.Fatalf("Put should have called again, should be 2 is: %d", bstore.PutCounter)
	}
}

func TestLazySessionInitialization(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bstore := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	bstore2 := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	bstore3 := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	session := offline.Exchange(bstore2)
	exchange := offline.Exchange(bstore3)
	sessionExch := &fakeSessionExchange{Interface: exchange, session: session}
	bservSessEx := NewWriteThrough(bstore, sessionExch)
	bgen := butil.NewBlockGenerator()

	block := bgen.Next()
	err := bstore.Put(ctx, block)
	if err != nil {
		t.Fatal(err)
	}
	block2 := bgen.Next()
	err = session.HasBlock(ctx, block2)
	if err != nil {
		t.Fatal(err)
	}

	bsession := NewSession(ctx, bservSessEx)
	if bsession.ses != nil {
		t.Fatal("Session exchange should not instantiated session immediately")
	}
	returnedBlock, err := bsession.GetBlock(ctx, block.Cid())
	if err != nil {
		t.Fatal("Should have fetched block locally")
	}
	if returnedBlock.Cid() != block.Cid() {
		t.Fatal("Got incorrect block")
	}
	if bsession.ses != nil {
		t.Fatal("Session exchange should not instantiated session if local store had block")
	}
	returnedBlock, err = bsession.GetBlock(ctx, block2.Cid())
	if err != nil {
		t.Fatal("Should have fetched block remotely")
	}
	if returnedBlock.Cid() != block2.Cid() {
		t.Fatal("Got incorrect block")
	}
	if bsession.ses != session {
		t.Fatal("Should have initialized session to fetch block")
	}
}

var _ blockstore.Blockstore = (*PutCountingBlockstore)(nil)

type PutCountingBlockstore struct {
	blockstore.Blockstore
	PutCounter int
}

func (bs *PutCountingBlockstore) Put(ctx context.Context, block blocks.Block) error {
	bs.PutCounter++
	return bs.Blockstore.Put(ctx, block)
}

var _ exchange.SessionExchange = (*fakeSessionExchange)(nil)

type fakeSessionExchange struct {
	exchange.Interface
	session exchange.Fetcher
}

func (fe *fakeSessionExchange) NewSession(ctx context.Context) exchange.Fetcher {
	if ctx == nil {
		panic("nil context")
	}
	return fe.session
}

func TestNilExchange(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bgen := butil.NewBlockGenerator()
	block := bgen.Next()

	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	bserv := NewWriteThrough(bs, nil)
	sess := NewSession(ctx, bserv)
	_, err := sess.GetBlock(ctx, block.Cid())
	if !ipld.IsNotFound(err) {
		t.Fatal("expected block to not be found")
	}
	err = bs.Put(ctx, block)
	if err != nil {
		t.Fatal(err)
	}
	b, err := sess.GetBlock(ctx, block.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if b.Cid() != block.Cid() {
		t.Fatal("got the wrong block")
	}
}
