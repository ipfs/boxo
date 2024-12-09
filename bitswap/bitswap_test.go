package bitswap_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/boxo/bitswap"
	bsmsg "github.com/ipfs/boxo/bitswap/message"
	"github.com/ipfs/boxo/bitswap/server"
	testinstance "github.com/ipfs/boxo/bitswap/testinstance"
	tn "github.com/ipfs/boxo/bitswap/testnet"
	mockrouting "github.com/ipfs/boxo/routing/mock"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	detectrace "github.com/ipfs/go-detect-race"
	delay "github.com/ipfs/go-ipfs-delay"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-test/random"
	tu "github.com/libp2p/go-libp2p-testing/etc"
	p2ptestutil "github.com/libp2p/go-libp2p-testing/netutil"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

const blockSize = 4

func isCI() bool {
	// https://github.blog/changelog/2020-04-15-github-actions-sets-the-ci-environment-variable-to-true/
	return os.Getenv("CI") != ""
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
}

// FIXME the tests are really sensitive to the network delay. fix them to work
// well under varying conditions
const kNetworkDelay = 0 * time.Millisecond

func TestClose(t *testing.T) {
	vnet := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(vnet, router, nil, nil)
	defer ig.Close()
	block := random.BlocksOfSize(1, blockSize)[0]
	bitswap := ig.Next()

	bitswap.Exchange.Close()
	_, err := bitswap.Exchange.GetBlock(context.Background(), block.Cid())
	if err == nil {
		t.Fatal("expected GetBlock to fail")
	}
}

func TestProviderForKeyButNetworkCannotFind(t *testing.T) { // TODO revisit this
	net := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(net, router, nil, nil)
	defer ig.Close()

	block := blocks.NewBlock([]byte("block"))
	pinfo := p2ptestutil.RandTestBogusIdentityOrFatal(t)
	err := router.Client(pinfo).Provide(context.Background(), block.Cid(), true) // but not on network
	if err != nil {
		t.Fatal(err)
	}

	solo := ig.Next()
	defer solo.Exchange.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	_, err = solo.Exchange.GetBlock(ctx, block.Cid())

	if err != context.DeadlineExceeded {
		t.Fatal("Expected DeadlineExceeded error")
	}
}

func TestGetBlockFromPeerAfterPeerAnnounces(t *testing.T) {
	net := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	block := blocks.NewBlock([]byte("block"))
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(net, router, nil, nil)
	defer ig.Close()

	peers := ig.Instances(2)
	hasBlock := peers[0]
	defer hasBlock.Exchange.Close()

	addBlock(t, context.Background(), hasBlock, block)

	wantsBlock := peers[1]
	defer wantsBlock.Exchange.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	received, err := wantsBlock.Exchange.GetBlock(ctx, block.Cid())
	if err != nil {
		t.Log(err)
		t.Fatal("Expected to succeed")
	}

	if !bytes.Equal(block.RawData(), received.RawData()) {
		t.Fatal("Data doesn't match")
	}
}

func TestDoesNotProvideWhenConfiguredNotTo(t *testing.T) {
	net := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	block := blocks.NewBlock([]byte("block"))
	bsOpts := []bitswap.Option{bitswap.ProviderSearchDelay(50 * time.Millisecond)}
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(net, router, nil, bsOpts)
	defer ig.Close()

	hasBlock := ig.Next()
	defer hasBlock.Exchange.Close()

	wantsBlock := ig.Next()
	defer wantsBlock.Exchange.Close()

	addBlock(t, context.Background(), hasBlock, block)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	ns := wantsBlock.Exchange.NewSession(ctx)

	received, err := ns.GetBlock(ctx, block.Cid())
	if received != nil {
		t.Fatalf("Expected to find nothing, found %s", received)
	}

	if err != context.DeadlineExceeded {
		t.Fatal("Expected deadline exceeded")
	}
}

// Tests that a received block is not stored in the blockstore if the block was
// not requested by the client
func TestUnwantedBlockNotAdded(t *testing.T) {
	net := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	block := blocks.NewBlock([]byte("block"))
	bsMessage := bsmsg.New(true)
	bsMessage.AddBlock(block)

	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(net, router, nil, nil)
	defer ig.Close()

	peers := ig.Instances(2)
	hasBlock := peers[0]
	defer hasBlock.Exchange.Close()

	addBlock(t, context.Background(), hasBlock, block)

	doesNotWantBlock := peers[1]
	defer doesNotWantBlock.Exchange.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	doesNotWantBlock.Exchange.ReceiveMessage(ctx, hasBlock.Identity.ID(), bsMessage)

	blockInStore, err := doesNotWantBlock.Blockstore.Has(ctx, block.Cid())
	if err != nil || blockInStore {
		t.Fatal("Unwanted block added to block store")
	}
}

// Tests that a received block is returned to the client and stored in the
// blockstore in the following scenario:
// - the want for the block has been requested by the client
// - the want for the block has not yet been sent out to a peer
//
//	(because the live request queue is full)
func TestPendingBlockAdded(t *testing.T) {
	ctx := context.Background()
	net := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	sessionBroadcastWantCapacity := 4

	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(net, router, nil, nil)
	defer ig.Close()

	instance := ig.Instances(1)[0]
	defer instance.Exchange.Close()

	oneSecCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Request enough blocks to exceed the session's broadcast want list
	// capacity (by one block). The session will put the remaining block
	// into the "tofetch" queue
	blks := random.BlocksOfSize(sessionBroadcastWantCapacity+1, blockSize)
	ks := make([]cid.Cid, 0, len(blks))
	for _, b := range blks {
		ks = append(ks, b.Cid())
	}
	outch, err := instance.Exchange.GetBlocks(ctx, ks)
	if err != nil {
		t.Fatal(err)
	}

	// Wait a little while to make sure the session has time to process the wants
	time.Sleep(time.Millisecond * 20)

	// Simulate receiving a message which contains the block in the "tofetch" queue
	lastBlock := blks[len(blks)-1]
	bsMessage := bsmsg.New(true)
	bsMessage.AddBlock(lastBlock)
	unknownPeer := peer.ID("QmUHfvCQrzyR6vFXmeyCptfCWedfcmfa12V6UuziDtrw23")
	instance.Exchange.ReceiveMessage(oneSecCtx, unknownPeer, bsMessage)

	// Make sure Bitswap adds the block to the output channel
	blkrecvd, ok := <-outch
	if !ok {
		t.Fatal("timed out waiting for block")
	}
	if !blkrecvd.Cid().Equals(lastBlock.Cid()) {
		t.Fatal("received wrong block")
	}
}

func TestLargeSwarm(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	numInstances := 100
	numBlocks := 2
	if detectrace.WithRace() {
		// when running with the race detector, 500 instances launches
		// well over 8k goroutines. This hits a race detector limit.
		numInstances = 20
	} else if isCI() {
		numInstances = 200
	} else {
		t.Parallel()
	}
	PerformDistributionTest(t, numInstances, numBlocks)
}

func TestLargeFile(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	if !isCI() {
		t.Parallel()
	}

	numInstances := 10
	numBlocks := 100
	PerformDistributionTest(t, numInstances, numBlocks)
}

func TestLargeFileTwoPeers(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	numInstances := 2
	numBlocks := 100
	PerformDistributionTest(t, numInstances, numBlocks)
}

func PerformDistributionTest(t *testing.T, numInstances, numBlocks int) {
	ctx := context.Background()
	if testing.Short() {
		t.SkipNow()
	}
	net := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(net, router, nil, []bitswap.Option{
		bitswap.TaskWorkerCount(5),
		bitswap.EngineTaskWorkerCount(5),
		bitswap.MaxOutstandingBytesPerPeer(1 << 20),
	})
	defer ig.Close()

	instances := ig.Instances(numInstances)
	blocks := random.BlocksOfSize(numBlocks, blockSize)

	t.Log("Give the blocks to the first instance")

	var blkeys []cid.Cid
	first := instances[0]
	for _, b := range blocks {
		blkeys = append(blkeys, b.Cid())
		addBlock(t, ctx, first, b)
	}

	t.Log("Distribute!")

	wg := sync.WaitGroup{}
	errs := make(chan error)

	for _, inst := range instances[1:] {
		wg.Add(1)
		go func(inst testinstance.Instance) {
			defer wg.Done()
			outch, err := inst.Exchange.GetBlocks(ctx, blkeys)
			if err != nil {
				errs <- err
			}
			for range outch {
			}
		}(inst)
	}

	go func() {
		wg.Wait()
		close(errs)
	}()

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TODO simplify this test. get to the _essence_!
func TestSendToWantingPeer(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	net := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(net, router, nil, nil)
	defer ig.Close()

	peers := ig.Instances(2)
	peerA := peers[0]
	peerB := peers[1]

	t.Logf("Session %v\n", peerA.Identity.ID())
	t.Logf("Session %v\n", peerB.Identity.ID())

	waitTime := time.Second * 5

	alpha := random.BlocksOfSize(1, blockSize)[0]
	// peerA requests and waits for block alpha
	ctx, cancel := context.WithTimeout(context.Background(), waitTime)
	defer cancel()
	alphaPromise, err := peerA.Exchange.GetBlocks(ctx, []cid.Cid{alpha.Cid()})
	if err != nil {
		t.Fatal(err)
	}

	// peerB announces to the network that he has block alpha
	addBlock(t, ctx, peerB, alpha)

	// At some point, peerA should get alpha (or timeout)
	blkrecvd, ok := <-alphaPromise
	if !ok {
		t.Fatal("context timed out and broke promise channel!")
	}

	if !blkrecvd.Cid().Equals(alpha.Cid()) {
		t.Fatal("Wrong block!")
	}
}

func TestEmptyKey(t *testing.T) {
	net := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(net, router, nil, nil)
	defer ig.Close()
	bs := ig.Instances(1)[0].Exchange

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	_, err := bs.GetBlock(ctx, cid.Cid{})
	if !ipld.IsNotFound(err) {
		t.Error("empty str key should return ErrNotFound")
	}
}

func assertStat(t *testing.T, st *bitswap.Stat, sblks, rblks, sdata, rdata uint64) {
	if sblks != st.BlocksSent {
		t.Errorf("mismatch in blocks sent: %d vs %d", sblks, st.BlocksSent)
	}

	if rblks != st.BlocksReceived {
		t.Errorf("mismatch in blocks recvd: %d vs %d", rblks, st.BlocksReceived)
	}

	if sdata != st.DataSent {
		t.Errorf("mismatch in data sent: %d vs %d", sdata, st.DataSent)
	}

	if rdata != st.DataReceived {
		t.Errorf("mismatch in data recvd: %d vs %d", rdata, st.DataReceived)
	}
}

func TestBasicBitswap(t *testing.T) {
	net := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(net, router, nil, nil)
	defer ig.Close()

	t.Log("Test a one node trying to get one block from another")

	instances := ig.Instances(3)
	blocks := random.BlocksOfSize(1, blockSize)

	// First peer has block
	addBlock(t, context.Background(), instances[0], blocks[0])

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// Second peer broadcasts want for block CID
	// (Received by first and third peers)
	blk, err := instances[1].Exchange.GetBlock(ctx, blocks[0].Cid())
	if err != nil {
		t.Fatal(err)
	}

	// When second peer receives block, it should send out a cancel, so third
	// peer should no longer keep second peer's want
	if err = tu.WaitFor(ctx, func() error {
		if len(instances[2].Exchange.WantlistForPeer(instances[1].Identity.ID())) != 0 {
			return errors.New("should have no items in other peers wantlist")
		}
		if len(instances[1].Exchange.GetWantlist()) != 0 {
			return errors.New("shouldnt have anything in wantlist")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	st0, err := instances[0].Exchange.Stat()
	if err != nil {
		t.Fatal(err)
	}
	st1, err := instances[1].Exchange.Stat()
	if err != nil {
		t.Fatal(err)
	}

	st2, err := instances[2].Exchange.Stat()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("stat node 0")
	assertStat(t, st0, 1, 0, uint64(len(blk.RawData())), 0)
	t.Log("stat node 1")
	assertStat(t, st1, 0, 1, 0, uint64(len(blk.RawData())))
	t.Log("stat node 2")
	assertStat(t, st2, 0, 0, 0, 0)

	if !bytes.Equal(blk.RawData(), blocks[0].RawData()) {
		t.Errorf("blocks aren't equal: expected %v, actual %v", blocks[0].RawData(), blk.RawData())
	}

	t.Log(blk)
	for _, inst := range instances {
		err := inst.Exchange.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDoubleGet(t *testing.T) {
	net := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(net, router, nil, nil)
	defer ig.Close()

	t.Log("Test a one node trying to get one block from another")

	instances := ig.Instances(2)
	blocks := random.BlocksOfSize(1, blockSize)

	// NOTE: A race condition can happen here where these GetBlocks requests go
	// through before the peers even get connected. This is okay, bitswap
	// *should* be able to handle this.
	ctx1, cancel1 := context.WithCancel(context.Background())
	blkch1, err := instances[1].Exchange.GetBlocks(ctx1, []cid.Cid{blocks[0].Cid()})
	if err != nil {
		t.Fatal(err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	blkch2, err := instances[1].Exchange.GetBlocks(ctx2, []cid.Cid{blocks[0].Cid()})
	if err != nil {
		t.Fatal(err)
	}

	// ensure both requests make it into the wantlist at the same time
	time.Sleep(time.Millisecond * 20)
	cancel1()

	_, ok := <-blkch1
	if ok {
		t.Fatal("expected channel to be closed")
	}

	addBlock(t, context.Background(), instances[0], blocks[0])

	select {
	case blk, ok := <-blkch2:
		if !ok {
			t.Fatal("expected to get the block here")
		}
		t.Log(blk)
	case <-time.After(time.Second * 5):
		p1wl := instances[0].Exchange.WantlistForPeer(instances[1].Identity.ID())
		if len(p1wl) != 1 {
			t.Logf("wantlist view didnt have 1 item (had %d)", len(p1wl))
		} else if !p1wl[0].Equals(blocks[0].Cid()) {
			t.Logf("had 1 item, it was wrong: %s %s", blocks[0].Cid(), p1wl[0])
		} else {
			t.Log("had correct wantlist, somehow")
		}
		t.Fatal("timed out waiting on block")
	}

	for _, inst := range instances {
		err := inst.Exchange.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestWantlistCleanup(t *testing.T) {
	net := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(net, router, nil, nil)
	defer ig.Close()

	instances := ig.Instances(2)
	instance := instances[0]
	bswap := instance.Exchange
	blocks := random.BlocksOfSize(20, blockSize)

	var keys []cid.Cid
	for _, b := range blocks {
		keys = append(keys, b.Cid())
	}

	// Once context times out, key should be removed from wantlist
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()
	_, err := bswap.GetBlock(ctx, keys[0])
	if err != context.DeadlineExceeded {
		t.Fatal("shouldnt have fetched any blocks")
	}

	time.Sleep(time.Millisecond * 50)

	if len(bswap.GetWantHaves()) > 0 {
		t.Fatal("should not have anyting in wantlist")
	}

	// Once context times out, keys should be removed from wantlist
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()
	_, err = bswap.GetBlocks(ctx, keys[:10])
	if err != nil {
		t.Fatal(err)
	}

	<-ctx.Done()
	time.Sleep(time.Millisecond * 50)

	if len(bswap.GetWantHaves()) > 0 {
		t.Fatal("should not have anyting in wantlist")
	}

	// Send want for single block, with no timeout
	_, err = bswap.GetBlocks(context.Background(), keys[:1])
	if err != nil {
		t.Fatal(err)
	}

	// Send want for 10 blocks
	ctx, cancel = context.WithCancel(context.Background())
	_, err = bswap.GetBlocks(ctx, keys[10:])
	if err != nil {
		t.Fatal(err)
	}

	// Even after 50 milli-seconds we haven't explicitly cancelled anything
	// and no timeouts have expired, so we should have 11 want-haves
	time.Sleep(time.Millisecond * 50)
	if len(bswap.GetWantHaves()) != 11 {
		t.Fatal("should have 11 keys in wantlist")
	}

	// Cancel the timeout for the request for 10 blocks. This should remove
	// the want-haves
	cancel()

	// Once the cancel is processed, we are left with the request for 1 block
	time.Sleep(time.Millisecond * 50)
	if !(len(bswap.GetWantHaves()) == 1 && bswap.GetWantHaves()[0] == keys[0]) {
		t.Fatal("should only have keys[0] in wantlist")
	}
}

func assertLedgerMatch(ra, rb *server.Receipt) error {
	if ra.Sent != rb.Recv {
		return fmt.Errorf("mismatch in ledgers (exchanged bytes): %d sent vs %d recvd", ra.Sent, rb.Recv)
	}

	if ra.Recv != rb.Sent {
		return fmt.Errorf("mismatch in ledgers (exchanged bytes): %d recvd vs %d sent", ra.Recv, rb.Sent)
	}

	if ra.Exchanged != rb.Exchanged {
		return fmt.Errorf("mismatch in ledgers (exchanged blocks): %d vs %d ", ra.Exchanged, rb.Exchanged)
	}

	return nil
}

func assertLedgerEqual(ra, rb *server.Receipt) error {
	if ra.Value != rb.Value {
		return fmt.Errorf("mismatch in ledgers (value/debt ratio): %f vs %f ", ra.Value, rb.Value)
	}

	if ra.Sent != rb.Sent {
		return fmt.Errorf("mismatch in ledgers (sent bytes): %d vs %d", ra.Sent, rb.Sent)
	}

	if ra.Recv != rb.Recv {
		return fmt.Errorf("mismatch in ledgers (recvd bytes): %d vs %d", ra.Recv, rb.Recv)
	}

	if ra.Exchanged != rb.Exchanged {
		return fmt.Errorf("mismatch in ledgers (exchanged blocks): %d vs %d ", ra.Exchanged, rb.Exchanged)
	}

	return nil
}

func newReceipt(sent, recv, exchanged uint64) *server.Receipt {
	return &server.Receipt{
		Peer:      "test",
		Value:     float64(sent) / (1 + float64(recv)),
		Sent:      sent,
		Recv:      recv,
		Exchanged: exchanged,
	}
}

func TestBitswapLedgerOneWay(t *testing.T) {
	net := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(net, router, nil, nil)
	defer ig.Close()

	t.Log("Test ledgers match when one peer sends block to another")

	instances := ig.Instances(2)
	blocks := random.BlocksOfSize(1, blockSize)
	addBlock(t, context.Background(), instances[0], blocks[0])

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	blk, err := instances[1].Exchange.GetBlock(ctx, blocks[0].Cid())
	if err != nil {
		t.Fatal(err)
	}

	ra := instances[0].Exchange.LedgerForPeer(instances[1].Identity.ID())
	rb := instances[1].Exchange.LedgerForPeer(instances[0].Identity.ID())

	// compare peer ledger receipts
	err = assertLedgerMatch(ra, rb)
	if err != nil {
		t.Fatal(err)
	}

	// check that receipts have intended values
	ratest := newReceipt(blockSize, 0, 1)
	err = assertLedgerEqual(ratest, ra)
	if err != nil {
		t.Fatal(err)
	}
	rbtest := newReceipt(0, blockSize, 1)
	err = assertLedgerEqual(rbtest, rb)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(blk)
	for _, inst := range instances {
		err := inst.Exchange.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestBitswapLedgerTwoWay(t *testing.T) {
	net := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	router := mockrouting.NewServer()
	ig := testinstance.NewTestInstanceGenerator(net, router, nil, nil)
	defer ig.Close()

	t.Log("Test ledgers match when two peers send one block to each other")

	instances := ig.Instances(2)
	blocks := random.BlocksOfSize(2, blockSize)
	addBlock(t, context.Background(), instances[0], blocks[0])
	addBlock(t, context.Background(), instances[1], blocks[1])

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	_, err := instances[1].Exchange.GetBlock(ctx, blocks[0].Cid())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	blk, err := instances[0].Exchange.GetBlock(ctx, blocks[1].Cid())
	if err != nil {
		t.Fatal(err)
	}

	ra := instances[0].Exchange.LedgerForPeer(instances[1].Identity.ID())
	rb := instances[1].Exchange.LedgerForPeer(instances[0].Identity.ID())

	// compare peer ledger receipts
	err = assertLedgerMatch(ra, rb)
	if err != nil {
		t.Fatal(err)
	}

	// check that receipts have intended values
	rtest := newReceipt(blockSize, blockSize, 2)
	err = assertLedgerEqual(rtest, ra)
	if err != nil {
		t.Fatal(err)
	}

	err = assertLedgerEqual(rtest, rb)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(blk)
	for _, inst := range instances {
		err := inst.Exchange.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

type testingScoreLedger struct {
	scorePeer server.ScorePeerFunc
	started   chan struct{}
	closed    chan struct{}
}

func newTestingScoreLedger() *testingScoreLedger {
	return &testingScoreLedger{
		nil,
		make(chan struct{}),
		make(chan struct{}),
	}
}

func (tsl *testingScoreLedger) GetReceipt(p peer.ID) *server.Receipt {
	return nil
}
func (tsl *testingScoreLedger) AddToSentBytes(p peer.ID, n int)     {}
func (tsl *testingScoreLedger) AddToReceivedBytes(p peer.ID, n int) {}
func (tsl *testingScoreLedger) PeerConnected(p peer.ID)             {}
func (tsl *testingScoreLedger) PeerDisconnected(p peer.ID)          {}
func (tsl *testingScoreLedger) Start(scorePeer server.ScorePeerFunc) {
	tsl.scorePeer = scorePeer
	close(tsl.started)
}

func (tsl *testingScoreLedger) Stop() {
	close(tsl.closed)
}

// Tests start and stop of a custom decision logic
func TestWithScoreLedger(t *testing.T) {
	tsl := newTestingScoreLedger()
	net := tn.VirtualNetwork(delay.Fixed(kNetworkDelay))
	router := mockrouting.NewServer()
	bsOpts := []bitswap.Option{bitswap.WithScoreLedger(tsl)}
	ig := testinstance.NewTestInstanceGenerator(net, router, nil, bsOpts)
	defer ig.Close()
	i := ig.Next()
	defer i.Exchange.Close()

	select {
	case <-tsl.started:
		if tsl.scorePeer == nil {
			t.Fatal("Expected the score function to be initialized")
		}
	case <-time.After(time.Second * 5):
		t.Fatal("Expected the score ledger to be started within 5s")
	}

	i.Exchange.Close()
	select {
	case <-tsl.closed:
	case <-time.After(time.Second * 5):
		t.Fatal("Expected the score ledger to be closed within 5s")
	}
}
