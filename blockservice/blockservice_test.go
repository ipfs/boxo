package blockservice

import (
	"context"
	"testing"

	blockstore "github.com/ipfs/boxo/blockstore"
	exchange "github.com/ipfs/boxo/exchange"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/verifcid"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-test/random"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
)

const blockSize = 4

func TestWriteThroughWorks(t *testing.T) {
	t.Parallel()

	bstore := &PutCountingBlockstore{
		blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore())),
		0,
	}
	exchbstore := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	exch := offline.Exchange(exchbstore)
	bserv := New(bstore, exch, WriteThrough(true))

	block := random.BlocksOfSize(1, blockSize)[0]

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

func TestExchangeWrite(t *testing.T) {
	t.Parallel()

	bstore := &PutCountingBlockstore{
		blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore())),
		0,
	}
	exchbstore := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	exch := &notifyCountingExchange{
		offline.Exchange(exchbstore),
		0,
	}
	bserv := New(bstore, exch, WriteThrough(true))

	for name, fetcher := range map[string]BlockGetter{
		"blockservice": bserv,
		"session":      NewSession(context.Background(), bserv),
	} {
		t.Run(name, func(t *testing.T) {
			// GetBlock
			blks := random.BlocksOfSize(3, blockSize)
			block := blks[0]
			err := exchbstore.Put(context.Background(), block)
			if err != nil {
				t.Fatal(err)
			}
			got, err := fetcher.GetBlock(context.Background(), block.Cid())
			if err != nil {
				t.Fatal(err)
			}
			if got.Cid() != block.Cid() {
				t.Fatalf("GetBlock returned unexpected block")
			}
			if bstore.PutCounter != 1 {
				t.Fatalf("expected one Put call, have: %d", bstore.PutCounter)
			}
			if exch.notifyCount != 1 {
				t.Fatalf("expected one NotifyNewBlocks call, have: %d", exch.notifyCount)
			}

			// GetBlocks
			b1 := blks[1]
			err = exchbstore.Put(context.Background(), b1)
			if err != nil {
				t.Fatal(err)
			}
			b2 := blks[2]
			err = exchbstore.Put(context.Background(), b2)
			if err != nil {
				t.Fatal(err)
			}
			bchan := fetcher.GetBlocks(context.Background(), []cid.Cid{b1.Cid(), b2.Cid()})
			var gotBlocks []blocks.Block
			for b := range bchan {
				gotBlocks = append(gotBlocks, b)
			}
			if len(gotBlocks) != 2 {
				t.Fatalf("expected to retrieve 2 blocks, got %d", len(gotBlocks))
			}
			if bstore.PutCounter != 3 {
				t.Fatalf("expected 3 Put call, have: %d", bstore.PutCounter)
			}
			if exch.notifyCount != 3 {
				t.Fatalf("expected one NotifyNewBlocks call, have: %d", exch.notifyCount)
			}

			// reset counts
			bstore.PutCounter = 0
			exch.notifyCount = 0
		})
	}
}

func TestLazySessionInitialization(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bstore := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	bstore2 := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	bstore3 := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	session := offline.Exchange(bstore2)
	exch := offline.Exchange(bstore3)
	sessionExch := &fakeSessionExchange{Interface: exch, session: session}
	bservSessEx := New(bstore, sessionExch, WriteThrough(true))
	blks := random.BlocksOfSize(2, blockSize)

	block := blks[0]
	err := bstore.Put(ctx, block)
	if err != nil {
		t.Fatal(err)
	}
	block2 := blks[1]
	err = bstore2.Put(ctx, block2)
	if err != nil {
		t.Fatal(err)
	}
	err = session.NotifyNewBlocks(ctx, block2)
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

func (bs *PutCountingBlockstore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	bs.PutCounter += len(blocks)
	return bs.Blockstore.PutMany(ctx, blocks)
}

var _ exchange.Interface = (*notifyCountingExchange)(nil)

type notifyCountingExchange struct {
	exchange.Interface
	notifyCount int
}

func (n *notifyCountingExchange) NotifyNewBlocks(ctx context.Context, blocks ...blocks.Block) error {
	n.notifyCount += len(blocks)
	return n.Interface.NotifyNewBlocks(ctx, blocks...)
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
	t.Parallel()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	block := random.BlocksOfSize(1, blockSize)[0]

	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	bserv := New(bs, nil, WriteThrough(true))
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

func TestAllowlist(t *testing.T) {
	t.Parallel()
	a := assert.New(t)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	block := random.BlocksOfSize(1, blockSize)[0]

	data := []byte("this is some blake3 block")
	mh, err := multihash.Sum(data, multihash.BLAKE3, -1)
	a.NoError(err)
	blake3 := cid.NewCidV1(cid.Raw, mh)

	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	a.NoError(bs.Put(ctx, block))
	b, err := blocks.NewBlockWithCid(data, blake3)
	a.NoError(err)
	a.NoError(bs.Put(ctx, b))

	check := func(getBlock func(context.Context, cid.Cid) (blocks.Block, error)) {
		_, err := getBlock(ctx, block.Cid())
		a.Error(err)
		a.ErrorIs(err, verifcid.ErrPossiblyInsecureHashFunction)

		_, err = getBlock(ctx, blake3)
		a.NoError(err)
	}

	blockservice := New(bs, nil, WithAllowlist(verifcid.NewAllowlist(map[uint64]bool{multihash.BLAKE3: true})))
	check(blockservice.GetBlock)
	check(NewSession(ctx, blockservice).GetBlock)
}

type fakeIsNewSessionCreateExchange struct {
	ses                 exchange.Fetcher
	newSessionWasCalled bool
}

var _ exchange.SessionExchange = (*fakeIsNewSessionCreateExchange)(nil)

func (*fakeIsNewSessionCreateExchange) Close() error {
	return nil
}

func (*fakeIsNewSessionCreateExchange) GetBlock(context.Context, cid.Cid) (blocks.Block, error) {
	panic("should call on the session")
}

func (*fakeIsNewSessionCreateExchange) GetBlocks(context.Context, []cid.Cid) (<-chan blocks.Block, error) {
	panic("should call on the session")
}

func (f *fakeIsNewSessionCreateExchange) NewSession(context.Context) exchange.Fetcher {
	f.newSessionWasCalled = true
	return f.ses
}

func (*fakeIsNewSessionCreateExchange) NotifyNewBlocks(context.Context, ...blocks.Block) error {
	return nil
}

func TestContextSession(t *testing.T) {
	t.Parallel()
	a := assert.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	blks := random.BlocksOfSize(2, blockSize)
	block1 := blks[0]
	block2 := blks[1]

	bs := blockstore.NewBlockstore(ds.NewMapDatastore())
	a.NoError(bs.Put(ctx, block1))
	a.NoError(bs.Put(ctx, block2))
	sesEx := &fakeIsNewSessionCreateExchange{ses: offline.Exchange(bs)}

	service := New(blockstore.NewBlockstore(ds.NewMapDatastore()), sesEx)

	ctx = ContextWithSession(ctx, service)

	b, err := service.GetBlock(ctx, block1.Cid())
	a.NoError(err)
	a.Equal(b.RawData(), block1.RawData())
	a.True(sesEx.newSessionWasCalled, "new session from context should be created")
	sesEx.newSessionWasCalled = false

	bchan := service.GetBlocks(ctx, []cid.Cid{block2.Cid()})
	a.Equal((<-bchan).RawData(), block2.RawData())
	a.False(sesEx.newSessionWasCalled, "session should be reused in context")

	a.Equal(
		NewSession(ctx, service),
		NewSession(ContextWithSession(ctx, service), service),
		"session must be deduped in all invocations on the same context",
	)
}
