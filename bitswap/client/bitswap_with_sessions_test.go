package client_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/boxo/bitswap"
	"github.com/ipfs/boxo/bitswap/client"
	"github.com/ipfs/boxo/bitswap/client/internal/session"
	"github.com/ipfs/boxo/bitswap/client/traceability"
	testinstance "github.com/ipfs/boxo/bitswap/testinstance"
	tn "github.com/ipfs/boxo/bitswap/testnet"
	mockrouting "github.com/ipfs/boxo/routing/mock"
	"github.com/ipfs/boxo/routing/providerquerymanager"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	delay "github.com/ipfs/go-ipfs-delay"
	"github.com/ipfs/go-test/random"
	tu "github.com/libp2p/go-libp2p-testing/etc"
	"github.com/libp2p/go-libp2p/core/peer"
)

const blockSize = 4

func getVirtualNetwork() tn.Network {
	// FIXME: the tests are really sensitive to the network delay. fix them to work
	// well under varying conditions
	return tn.VirtualNetwork(delay.Fixed(0))
}

func addBlock(t *testing.T, ctx context.Context, inst testinstance.Instance, blk blocks.Block) {
	t.Helper()
	err := inst.Blockstore.Put(ctx, blk)
	if err != nil {
		t.Fatal(err)
	}
	err = inst.Exchange.NotifyNewBlocks(ctx, blk)
	if err != nil {
		t.Fatal(err)
	}
	err = inst.Routing.Provide(ctx, blk.Cid(), true)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBasicSessions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vnet := getVirtualNetwork()
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(vnet, router, nil, nil)
	defer ig.Close()

	block := random.BlocksOfSize(1, blockSize)[0]
	inst := ig.Instances(2)

	a := inst[0]
	b := inst[1]

	// Add a block to Peer B
	if err := b.Blockstore.Put(ctx, block); err != nil {
		t.Fatal(err)
	}

	// Create a session on Peer A
	sesa := a.Exchange.NewSession(ctx)

	// Get the block
	blkout, err := sesa.GetBlock(ctx, block.Cid())
	if err != nil {
		t.Fatal(err)
	}

	if !blkout.Cid().Equals(block.Cid()) {
		t.Fatal("got wrong block")
	}

	traceBlock, ok := blkout.(traceability.Block)
	if !ok {
		t.Fatal("did not get tracable block")
	}

	if traceBlock.From != b.Identity.ID() {
		t.Fatal("should have received block from peer B, did not")
	}
}

func assertBlockListsFrom(from peer.ID, got, exp []blocks.Block) error {
	if len(got) != len(exp) {
		return fmt.Errorf("got wrong number of blocks, %d != %d", len(got), len(exp))
	}

	h := cid.NewSet()
	for _, b := range got {
		h.Add(b.Cid())
		traceableBlock, ok := b.(traceability.Block)
		if !ok {
			return fmt.Errorf("not a traceable block: %s", b.Cid())
		}
		if traceableBlock.From != from {
			return fmt.Errorf("incorrect peer sent block, expect %s, got %s", from, traceableBlock.From)
		}
	}
	for _, b := range exp {
		if !h.Has(b.Cid()) {
			return fmt.Errorf("didnt have: %s", b.Cid())
		}
	}
	return nil
}

// TestCustomProviderQueryManager tests that nothing breaks if we use a custom
// PQM when creating bitswap.
func TestCustomProviderQueryManager(t *testing.T) {
	vnet := getVirtualNetwork()
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(vnet, router, nil, nil)
	defer ig.Close()

	block := random.BlocksOfSize(1, blockSize)[0]
	a := ig.Next()
	b := ig.Next()

	// Replace bitswap in instance a with our customized one.
	pqm, err := providerquerymanager.New(a.Adapter, router.Client(a.Identity))
	if err != nil {
		t.Fatal(err)
	}
	defer pqm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs := bitswap.New(ctx, a.Adapter, pqm, a.Blockstore,
		bitswap.WithClientOption(client.WithDefaultProviderQueryManager(false)))
	a.Exchange.Close() // close old to be sure.
	a.Exchange = bs
	// Connect instances only after bitswap exists.
	testinstance.ConnectInstances([]testinstance.Instance{a, b})

	// Add a block to Peer B
	if err := b.Blockstore.Put(ctx, block); err != nil {
		t.Fatal(err)
	}

	// Create a session on Peer A
	sesa := a.Exchange.NewSession(ctx)

	// Get the block
	blkout, err := sesa.GetBlock(ctx, block.Cid())
	if err != nil {
		t.Fatal(err)
	}

	if !blkout.Cid().Equals(block.Cid()) {
		t.Fatal("got wrong block")
	}

	traceBlock, ok := blkout.(traceability.Block)
	if !ok {
		t.Fatal("did not get tracable block")
	}

	if traceBlock.From != b.Identity.ID() {
		t.Fatal("should have received block from peer B, did not")
	}
}

func TestSessionBetweenPeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vnet := tn.VirtualNetwork(delay.Fixed(time.Millisecond))
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(vnet, router, nil, []bitswap.Option{bitswap.SetSimulateDontHavesOnTimeout(false)})
	defer ig.Close()

	inst := ig.Instances(10)

	// Add 101 blocks to Peer A
	blks := random.BlocksOfSize(101, blockSize)
	if err := inst[0].Blockstore.PutMany(ctx, blks); err != nil {
		t.Fatal(err)
	}

	var cids []cid.Cid
	for _, blk := range blks {
		cids = append(cids, blk.Cid())
	}

	// Create a session on Peer B
	ses := inst[1].Exchange.NewSession(ctx)
	if _, err := ses.GetBlock(ctx, cids[0]); err != nil {
		t.Fatal(err)
	}
	blks = blks[1:]
	cids = cids[1:]

	// Fetch blocks with the session, 10 at a time
	for i := 0; i < 10; i++ {
		ch, err := ses.GetBlocks(ctx, cids[i*10:(i+1)*10])
		if err != nil {
			t.Fatal(err)
		}

		var got []blocks.Block
		for b := range ch {
			got = append(got, b)
		}
		if err := assertBlockListsFrom(inst[0].Identity.ID(), got, blks[i*10:(i+1)*10]); err != nil {
			t.Fatal(err)
		}
	}

	// Uninvolved nodes should receive
	// - initial broadcast want-have of root block
	// - CANCEL (when Peer A receives the root block from Peer B)
	for _, is := range inst[2:] {
		stat, err := is.Exchange.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if stat.MessagesReceived > 2 {
			t.Fatal("uninvolved nodes should only receive two messages", stat.MessagesReceived)
		}
	}
}

func TestSessionSplitFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vnet := getVirtualNetwork()
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(vnet, router, nil, nil)
	defer ig.Close()

	inst := ig.Instances(11)

	// Add 10 distinct blocks to each of 10 peers
	blks := random.BlocksOfSize(100, blockSize)
	for i := 0; i < 10; i++ {
		if err := inst[i].Blockstore.PutMany(ctx, blks[i*10:(i+1)*10]); err != nil {
			t.Fatal(err)
		}
	}

	var cids []cid.Cid
	for _, blk := range blks {
		cids = append(cids, blk.Cid())
	}

	// Create a session on the remaining peer and fetch all the blocks 10 at a time
	ses := inst[10].Exchange.NewSession(ctx).(*session.Session)
	ses.SetBaseTickDelay(time.Millisecond * 10)

	for i := 0; i < 10; i++ {
		ch, err := ses.GetBlocks(ctx, cids[i*10:(i+1)*10])
		if err != nil {
			t.Fatal(err)
		}

		var got []blocks.Block
		for b := range ch {
			got = append(got, b)
		}
		if err := assertBlockListsFrom(inst[i].Identity.ID(), got, blks[i*10:(i+1)*10]); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFetchNotConnected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	vnet := getVirtualNetwork()
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(vnet, router, nil, []bitswap.Option{bitswap.ProviderSearchDelay(10 * time.Millisecond)})
	defer ig.Close()

	other := ig.Next()

	// Provide 10 blocks on Peer A
	blks := random.BlocksOfSize(10, blockSize)
	for _, block := range blks {
		addBlock(t, ctx, other, block)
	}

	var cids []cid.Cid
	for _, blk := range blks {
		cids = append(cids, blk.Cid())
	}

	// Request blocks with Peer B
	// Note: Peer A and Peer B are not initially connected, so this tests
	// that Peer B will search for and find Peer A
	thisNode := ig.Next()
	ses := thisNode.Exchange.NewSession(ctx).(*session.Session)
	ses.SetBaseTickDelay(time.Millisecond * 10)
	ch, err := ses.GetBlocks(ctx, cids)
	if err != nil {
		t.Fatal(err)
	}

	var got []blocks.Block
	for b := range ch {
		got = append(got, b)
	}
	if err := assertBlockListsFrom(other.Identity.ID(), got, blks); err != nil {
		t.Fatal(err)
	}
}

func TestFetchAfterDisconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	vnet := getVirtualNetwork()
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(vnet, router, nil, []bitswap.Option{
		bitswap.ProviderSearchDelay(10 * time.Millisecond),
		bitswap.RebroadcastDelay(delay.Fixed(15 * time.Millisecond)),
	})
	defer ig.Close()

	inst := ig.Instances(2)
	peerA := inst[0]
	peerB := inst[1]

	// Provide 5 blocks on Peer A
	blks := random.BlocksOfSize(10, blockSize)
	var cids []cid.Cid
	for _, blk := range blks {
		cids = append(cids, blk.Cid())
	}

	firstBlks := blks[:5]
	for _, block := range firstBlks {
		addBlock(t, ctx, peerA, block)
	}

	// Request all blocks with Peer B
	ses := peerB.Exchange.NewSession(ctx).(*session.Session)
	ses.SetBaseTickDelay(time.Millisecond * 10)

	ch, err := ses.GetBlocks(ctx, cids)
	if err != nil {
		t.Fatal(err)
	}

	// Should get first 5 blocks
	var got []blocks.Block
	for i := 0; i < 5; i++ {
		b := <-ch
		got = append(got, b)
	}

	if err := assertBlockListsFrom(peerA.Identity.ID(), got, blks[:5]); err != nil {
		t.Fatal(err)
	}

	// Break connection
	err = peerA.Adapter.DisconnectFrom(ctx, peerB.Identity.ID())
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(20 * time.Millisecond)

	// Provide remaining blocks
	lastBlks := blks[5:]
	for _, block := range lastBlks {
		addBlock(t, ctx, peerA, block)
	}

	// Peer B should call FindProviders() and find Peer A

	// Should get last 5 blocks
	for i := 0; i < 5; i++ {
		select {
		case b := <-ch:
			got = append(got, b)
		case <-ctx.Done():
		}
	}

	if err := assertBlockListsFrom(peerA.Identity.ID(), got, blks); err != nil {
		t.Fatal(err)
	}
}

func TestInterestCacheOverflow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vnet := getVirtualNetwork()
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(vnet, router, nil, nil)
	defer ig.Close()

	blks := random.BlocksOfSize(2049, blockSize)
	inst := ig.Instances(2)

	a := inst[0]
	b := inst[1]

	ses := a.Exchange.NewSession(ctx)
	zeroch, err := ses.GetBlocks(ctx, []cid.Cid{blks[0].Cid()})
	if err != nil {
		t.Fatal(err)
	}

	var restcids []cid.Cid
	for _, blk := range blks[1:] {
		restcids = append(restcids, blk.Cid())
	}

	restch, err := ses.GetBlocks(ctx, restcids)
	if err != nil {
		t.Fatal(err)
	}

	// wait to ensure that all the above cids were added to the sessions cache
	time.Sleep(time.Millisecond * 50)

	addBlock(t, ctx, b, blks[0])

	select {
	case blk, ok := <-zeroch:
		if ok && blk.Cid().Equals(blks[0].Cid()) {
			// success!
		} else {
			t.Fatal("failed to get the block")
		}
	case <-restch:
		t.Fatal("should not get anything on restch")
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for block")
	}
}

func TestPutAfterSessionCacheEvict(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vnet := getVirtualNetwork()
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(vnet, router, nil, nil)
	defer ig.Close()

	blks := random.BlocksOfSize(2500, blockSize)
	inst := ig.Instances(1)

	a := inst[0]

	ses := a.Exchange.NewSession(ctx)

	var allcids []cid.Cid
	for _, blk := range blks[1:] {
		allcids = append(allcids, blk.Cid())
	}

	blkch, err := ses.GetBlocks(ctx, allcids)
	if err != nil {
		t.Fatal(err)
	}

	// wait to ensure that all the above cids were added to the sessions cache
	time.Sleep(time.Millisecond * 50)

	addBlock(t, ctx, a, blks[17])

	select {
	case <-blkch:
	case <-time.After(time.Millisecond * 50):
		t.Fatal("timed out waiting for block")
	}
}

func TestMultipleSessions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vnet := getVirtualNetwork()
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(vnet, router, nil, nil)
	defer ig.Close()

	blk := random.BlocksOfSize(1, blockSize)[0]
	inst := ig.Instances(2)

	a := inst[0]
	b := inst[1]

	ctx1, cancel1 := context.WithCancel(ctx)
	ses := a.Exchange.NewSession(ctx1)

	blkch, err := ses.GetBlocks(ctx, []cid.Cid{blk.Cid()})
	if err != nil {
		t.Fatal(err)
	}
	cancel1()

	ses2 := a.Exchange.NewSession(ctx)
	blkch2, err := ses2.GetBlocks(ctx, []cid.Cid{blk.Cid()})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 10)
	addBlock(t, ctx, b, blk)

	select {
	case <-blkch2:
	case <-time.After(time.Second * 20):
		t.Fatal("bad juju")
	}
	_ = blkch
}

func TestWantlistClearsOnCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	vnet := getVirtualNetwork()
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(vnet, router, nil, nil)
	defer ig.Close()

	blks := random.BlocksOfSize(10, blockSize)
	var cids []cid.Cid
	for _, blk := range blks {
		cids = append(cids, blk.Cid())
	}

	inst := ig.Instances(1)

	a := inst[0]

	ctx1, cancel1 := context.WithCancel(ctx)
	ses := a.Exchange.NewSession(ctx1)

	_, err := ses.GetBlocks(ctx, cids)
	if err != nil {
		t.Fatal(err)
	}
	cancel1()

	if err := tu.WaitFor(ctx, func() error {
		if len(a.Exchange.GetWantlist()) > 0 {
			return errors.New("expected empty wantlist")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDontHaveTimeoutConfig(t *testing.T) {
	cfg := client.DefaultDontHaveTimeoutConfig()
	if cfg.DontHaveTimeout <= 0 {
		t.Fatal("invalid default dont have timeout")
	}

	vnet := getVirtualNetwork()
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(vnet, router, nil, nil)
	defer ig.Close()

	a := ig.Next()

	// Replace bitswap in instance a with our customized one.
	pqm, err := providerquerymanager.New(a.Adapter, router.Client(a.Identity))
	if err != nil {
		t.Fatal(err)
	}
	defer pqm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs := bitswap.New(ctx, a.Adapter, pqm, a.Blockstore,
		bitswap.WithClientOption(client.WithDontHaveTimeoutConfig(cfg)))
	a.Exchange.Close()
	a.Exchange = bs

	a.Exchange.Close()
	bs.Close()
}
