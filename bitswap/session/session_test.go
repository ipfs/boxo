package session

import (
	"context"
	"sync"
	"testing"
	"time"

	notifications "github.com/ipfs/go-bitswap/notifications"
	bssd "github.com/ipfs/go-bitswap/sessiondata"
	"github.com/ipfs/go-bitswap/testutil"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	delay "github.com/ipfs/go-ipfs-delay"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type wantReq struct {
	cids  []cid.Cid
	peers []peer.ID
}

type fakeWantManager struct {
	wantReqs   chan wantReq
	cancelReqs chan wantReq
}

func (fwm *fakeWantManager) WantBlocks(ctx context.Context, cids []cid.Cid, peers []peer.ID, ses uint64) {
	select {
	case fwm.wantReqs <- wantReq{cids, peers}:
	case <-ctx.Done():
	}
}

func (fwm *fakeWantManager) CancelWants(ctx context.Context, cids []cid.Cid, peers []peer.ID, ses uint64) {
	select {
	case fwm.cancelReqs <- wantReq{cids, peers}:
	case <-ctx.Done():
	}
}

type fakePeerManager struct {
	lk                     sync.RWMutex
	peers                  []peer.ID
	findMorePeersRequested chan cid.Cid
}

func (fpm *fakePeerManager) FindMorePeers(ctx context.Context, k cid.Cid) {
	select {
	case fpm.findMorePeersRequested <- k:
	case <-ctx.Done():
	}
}

func (fpm *fakePeerManager) GetOptimizedPeers() []bssd.OptimizedPeer {
	fpm.lk.Lock()
	defer fpm.lk.Unlock()
	optimizedPeers := make([]bssd.OptimizedPeer, 0, len(fpm.peers))
	for _, peer := range fpm.peers {
		optimizedPeers = append(optimizedPeers, bssd.OptimizedPeer{Peer: peer, OptimizationRating: 1.0})
	}
	return optimizedPeers
}

func (fpm *fakePeerManager) RecordPeerRequests([]peer.ID, []cid.Cid) {}
func (fpm *fakePeerManager) RecordPeerResponse(p peer.ID, c []cid.Cid) {
	fpm.lk.Lock()
	fpm.peers = append(fpm.peers, p)
	fpm.lk.Unlock()
}
func (fpm *fakePeerManager) RecordCancels(c []cid.Cid) {}

type fakeRequestSplitter struct {
}

func (frs *fakeRequestSplitter) SplitRequest(optimizedPeers []bssd.OptimizedPeer, keys []cid.Cid) []bssd.PartialRequest {
	peers := make([]peer.ID, len(optimizedPeers))
	for i, optimizedPeer := range optimizedPeers {
		peers[i] = optimizedPeer.Peer
	}
	return []bssd.PartialRequest{bssd.PartialRequest{Peers: peers, Keys: keys}}
}

func (frs *fakeRequestSplitter) RecordDuplicateBlock() {}
func (frs *fakeRequestSplitter) RecordUniqueBlock()    {}

func TestSessionGetBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	wantReqs := make(chan wantReq, 1)
	cancelReqs := make(chan wantReq, 1)
	fwm := &fakeWantManager{wantReqs, cancelReqs}
	fpm := &fakePeerManager{}
	frs := &fakeRequestSplitter{}
	notif := notifications.New()
	defer notif.Shutdown()
	id := testutil.GenerateSessionID()
	session := New(ctx, id, fwm, fpm, frs, notif, time.Second, delay.Fixed(time.Minute))
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(broadcastLiveWantsLimit * 2)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	getBlocksCh, err := session.GetBlocks(ctx, cids)

	if err != nil {
		t.Fatal("error getting blocks")
	}

	// check initial want request
	receivedWantReq := <-fwm.wantReqs

	if len(receivedWantReq.cids) != broadcastLiveWantsLimit {
		t.Fatal("did not enqueue correct initial number of wants")
	}
	if receivedWantReq.peers != nil {
		t.Fatal("first want request should be a broadcast")
	}
	for _, c := range cids {
		if !session.IsWanted(c) {
			t.Fatal("expected session to want cids")
		}
	}

	// now receive the first set of blocks
	peers := testutil.GeneratePeers(broadcastLiveWantsLimit)
	var newCancelReqs []wantReq
	var newBlockReqs []wantReq
	var receivedBlocks []blocks.Block
	for i, p := range peers {
		// simulate what bitswap does on receiving a message:
		// - calls ReceiveFrom() on session
		// - publishes block to pubsub channel
		blk := blks[testutil.IndexOf(blks, receivedWantReq.cids[i])]
		session.ReceiveFrom(p, []cid.Cid{blk.Cid()})
		notif.Publish(blk)

		select {
		case cancelBlock := <-cancelReqs:
			newCancelReqs = append(newCancelReqs, cancelBlock)
		case <-ctx.Done():
			t.Fatal("did not cancel block want")
		}

		select {
		case receivedBlock := <-getBlocksCh:
			receivedBlocks = append(receivedBlocks, receivedBlock)
		case <-ctx.Done():
			t.Fatal("Did not receive block!")
		}

		select {
		case wantBlock := <-wantReqs:
			newBlockReqs = append(newBlockReqs, wantBlock)
		default:
		}
	}

	// verify new peers were recorded
	fpm.lk.Lock()
	if len(fpm.peers) != broadcastLiveWantsLimit {
		t.Fatal("received blocks not recorded by the peer manager")
	}
	for _, p := range fpm.peers {
		if !testutil.ContainsPeer(peers, p) {
			t.Fatal("incorrect peer recorded to peer manager")
		}
	}
	fpm.lk.Unlock()

	// look at new interactions with want manager

	// should have cancelled each received block
	if len(newCancelReqs) != broadcastLiveWantsLimit {
		t.Fatal("did not cancel each block once it was received")
	}
	// new session reqs should be targeted
	var newCidsRequested []cid.Cid
	for _, w := range newBlockReqs {
		if len(w.peers) == 0 {
			t.Fatal("should not have broadcast again after initial broadcast")
		}
		newCidsRequested = append(newCidsRequested, w.cids...)
	}

	// full new round of cids should be requested
	if len(newCidsRequested) != broadcastLiveWantsLimit {
		t.Fatal("new blocks were not requested")
	}

	// receive remaining blocks
	for i, p := range peers {
		// simulate what bitswap does on receiving a message:
		// - calls ReceiveFrom() on session
		// - publishes block to pubsub channel
		blk := blks[testutil.IndexOf(blks, newCidsRequested[i])]
		session.ReceiveFrom(p, []cid.Cid{blk.Cid()})
		notif.Publish(blk)

		receivedBlock := <-getBlocksCh
		receivedBlocks = append(receivedBlocks, receivedBlock)
		cancelBlock := <-cancelReqs
		newCancelReqs = append(newCancelReqs, cancelBlock)
	}

	if len(receivedBlocks) != len(blks) {
		t.Fatal("did not receive enough blocks")
	}
	if len(newCancelReqs) != len(receivedBlocks) {
		t.Fatal("expected an equal number of received blocks and cancels")
	}
	for _, block := range receivedBlocks {
		if !testutil.ContainsBlock(blks, block) {
			t.Fatal("received incorrect block")
		}
	}
	for _, c := range cids {
		if session.IsWanted(c) {
			t.Fatal("expected session NOT to want cids")
		}
	}
}

func TestSessionFindMorePeers(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	wantReqs := make(chan wantReq, 1)
	cancelReqs := make(chan wantReq, 1)
	fwm := &fakeWantManager{wantReqs, cancelReqs}
	fpm := &fakePeerManager{findMorePeersRequested: make(chan cid.Cid, 1)}
	frs := &fakeRequestSplitter{}
	notif := notifications.New()
	defer notif.Shutdown()
	id := testutil.GenerateSessionID()
	session := New(ctx, id, fwm, fpm, frs, notif, time.Second, delay.Fixed(time.Minute))
	session.SetBaseTickDelay(200 * time.Microsecond)
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(broadcastLiveWantsLimit * 2)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	getBlocksCh, err := session.GetBlocks(ctx, cids)
	if err != nil {
		t.Fatal("error getting blocks")
	}

	// clear the initial block of wants
	select {
	case <-wantReqs:
	case <-ctx.Done():
		t.Fatal("Did not make first want request ")
	}

	// receive a block to trigger a tick reset
	time.Sleep(20 * time.Millisecond) // need to make sure some latency registers
	// or there will be no tick set -- time precision on Windows in go is in the
	// millisecond range
	p := testutil.GeneratePeers(1)[0]

	// simulate what bitswap does on receiving a message:
	// - calls ReceiveFrom() on session
	// - publishes block to pubsub channel
	blk := blks[0]
	session.ReceiveFrom(p, []cid.Cid{blk.Cid()})
	notif.Publish(blk)
	select {
	case <-cancelReqs:
	case <-ctx.Done():
		t.Fatal("Did not cancel block")
	}
	select {
	case <-getBlocksCh:
	case <-ctx.Done():
		t.Fatal("Did not get block")
	}
	select {
	case <-wantReqs:
	case <-ctx.Done():
		t.Fatal("Did not make second want request ")
	}

	// verify a broadcast was made
	select {
	case receivedWantReq := <-wantReqs:
		if len(receivedWantReq.cids) < broadcastLiveWantsLimit {
			t.Fatal("did not rebroadcast whole live list")
		}
		if receivedWantReq.peers != nil {
			t.Fatal("did not make a broadcast")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}

	// wait for a request to get more peers to occur
	select {
	case <-fpm.findMorePeersRequested:
	case <-ctx.Done():
		t.Fatal("Did not find more peers")
	}
}

func TestSessionFailingToGetFirstBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	wantReqs := make(chan wantReq, 1)
	cancelReqs := make(chan wantReq, 1)
	fwm := &fakeWantManager{wantReqs, cancelReqs}
	fpm := &fakePeerManager{findMorePeersRequested: make(chan cid.Cid, 1)}
	frs := &fakeRequestSplitter{}
	notif := notifications.New()
	defer notif.Shutdown()
	id := testutil.GenerateSessionID()

	session := New(ctx, id, fwm, fpm, frs, notif, 10*time.Millisecond, delay.Fixed(100*time.Millisecond))
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(4)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	startTick := time.Now()
	_, err := session.GetBlocks(ctx, cids)
	if err != nil {
		t.Fatal("error getting blocks")
	}

	// clear the initial block of wants
	select {
	case <-wantReqs:
	case <-ctx.Done():
		t.Fatal("Did not make first want request ")
	}

	// verify a broadcast is made
	select {
	case receivedWantReq := <-wantReqs:
		if len(receivedWantReq.cids) < len(cids) {
			t.Fatal("did not rebroadcast whole live list")
		}
		if receivedWantReq.peers != nil {
			t.Fatal("did not make a broadcast")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}

	// wait for a request to get more peers to occur
	select {
	case k := <-fpm.findMorePeersRequested:
		if testutil.IndexOf(blks, k) == -1 {
			t.Fatal("did not rebroadcast an active want")
		}
	case <-ctx.Done():
		t.Fatal("Did not find more peers")
	}
	firstTickLength := time.Since(startTick)

	// wait for another broadcast to occur
	select {
	case receivedWantReq := <-wantReqs:
		if len(receivedWantReq.cids) < len(cids) {
			t.Fatal("did not rebroadcast whole live list")
		}
		if receivedWantReq.peers != nil {
			t.Fatal("did not make a broadcast")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}
	startTick = time.Now()
	// wait for another broadcast to occur
	select {
	case receivedWantReq := <-wantReqs:
		if len(receivedWantReq.cids) < len(cids) {
			t.Fatal("did not rebroadcast whole live list")
		}
		if receivedWantReq.peers != nil {
			t.Fatal("did not make a broadcast")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}
	consecutiveTickLength := time.Since(startTick)
	// tick should take longer
	if firstTickLength > consecutiveTickLength {
		t.Fatal("Should have increased tick length after first consecutive tick")
	}
	startTick = time.Now()
	// wait for another broadcast to occur
	select {
	case receivedWantReq := <-wantReqs:
		if len(receivedWantReq.cids) < len(cids) {
			t.Fatal("did not rebroadcast whole live list")
		}
		if receivedWantReq.peers != nil {
			t.Fatal("did not make a broadcast")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}
	secondConsecutiveTickLength := time.Since(startTick)
	// tick should take longer
	if consecutiveTickLength > secondConsecutiveTickLength {
		t.Fatal("Should have increased tick length after first consecutive tick")
	}

	// should not have looked for peers on consecutive ticks
	select {
	case <-fpm.findMorePeersRequested:
		t.Fatal("Should not have looked for peers on consecutive tick")
	default:
	}

	// wait for rebroadcast to occur
	select {
	case k := <-fpm.findMorePeersRequested:
		if testutil.IndexOf(blks, k) == -1 {
			t.Fatal("did not rebroadcast an active want")
		}
	case <-ctx.Done():
		t.Fatal("Did not rebroadcast to find more peers")
	}
}

func TestSessionCtxCancelClosesGetBlocksChannel(t *testing.T) {
	wantReqs := make(chan wantReq, 1)
	cancelReqs := make(chan wantReq, 1)
	fwm := &fakeWantManager{wantReqs, cancelReqs}
	fpm := &fakePeerManager{}
	frs := &fakeRequestSplitter{}
	notif := notifications.New()
	defer notif.Shutdown()
	id := testutil.GenerateSessionID()

	// Create a new session with its own context
	sessctx, sesscancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	session := New(sessctx, id, fwm, fpm, frs, notif, time.Second, delay.Fixed(time.Minute))

	timerCtx, timerCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer timerCancel()

	// Request a block with a new context
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(1)
	getctx, getcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer getcancel()

	getBlocksCh, err := session.GetBlocks(getctx, []cid.Cid{blks[0].Cid()})
	if err != nil {
		t.Fatal("error getting blocks")
	}

	// Cancel the session context
	sesscancel()

	// Expect the GetBlocks() channel to be closed
	select {
	case _, ok := <-getBlocksCh:
		if ok {
			t.Fatal("expected channel to be closed but was not closed")
		}
	case <-timerCtx.Done():
		t.Fatal("expected channel to be closed before timeout")
	}
}
