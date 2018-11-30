package session

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-block-format"

	"github.com/ipfs/go-bitswap/testutil"
	cid "github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	peer "github.com/libp2p/go-libp2p-peer"
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
	fwm.wantReqs <- wantReq{cids, peers}
}

func (fwm *fakeWantManager) CancelWants(ctx context.Context, cids []cid.Cid, peers []peer.ID, ses uint64) {
	fwm.cancelReqs <- wantReq{cids, peers}
}

type fakePeerManager struct {
	lk                     sync.RWMutex
	peers                  []peer.ID
	findMorePeersRequested bool
}

func (fpm *fakePeerManager) FindMorePeers(context.Context, cid.Cid) {
	fpm.lk.Lock()
	fpm.findMorePeersRequested = true
	fpm.lk.Unlock()
}

func (fpm *fakePeerManager) GetOptimizedPeers() []peer.ID {
	fpm.lk.Lock()
	defer fpm.lk.Unlock()
	return fpm.peers
}

func (fpm *fakePeerManager) RecordPeerRequests([]peer.ID, []cid.Cid) {}
func (fpm *fakePeerManager) RecordPeerResponse(p peer.ID, c cid.Cid) {
	fpm.lk.Lock()
	fpm.peers = append(fpm.peers, p)
	fpm.lk.Unlock()
}

func TestSessionGetBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	wantReqs := make(chan wantReq, 1)
	cancelReqs := make(chan wantReq, 1)
	fwm := &fakeWantManager{wantReqs, cancelReqs}
	fpm := &fakePeerManager{}
	id := testutil.GenerateSessionID()
	session := New(ctx, id, fwm, fpm)
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(activeWantsLimit * 2)
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

	if len(receivedWantReq.cids) != activeWantsLimit {
		t.Fatal("did not enqueue correct initial number of wants")
	}
	if receivedWantReq.peers != nil {
		t.Fatal("first want request should be a broadcast")
	}

	// now receive the first set of blocks
	peers := testutil.GeneratePeers(activeWantsLimit)
	var newCancelReqs []wantReq
	var newBlockReqs []wantReq
	var receivedBlocks []blocks.Block
	for i, p := range peers {
		session.ReceiveBlockFrom(p, blks[testutil.IndexOf(blks, receivedWantReq.cids[i])])
		receivedBlock := <-getBlocksCh
		receivedBlocks = append(receivedBlocks, receivedBlock)
		cancelBlock := <-cancelReqs
		newCancelReqs = append(newCancelReqs, cancelBlock)
		wantBlock := <-wantReqs
		newBlockReqs = append(newBlockReqs, wantBlock)
	}

	// verify new peers were recorded
	fpm.lk.Lock()
	if len(fpm.peers) != activeWantsLimit {
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
	if len(newCancelReqs) != activeWantsLimit {
		t.Fatal("did not cancel each block once it was received")
	}
	// new session reqs should be targeted
	totalEnqueued := 0
	for _, w := range newBlockReqs {
		if len(w.peers) == 0 {
			t.Fatal("should not have broadcast again after initial broadcast")
		}
		totalEnqueued += len(w.cids)
	}

	// full new round of cids should be requested
	if totalEnqueued != activeWantsLimit {
		t.Fatal("new blocks were not requested")
	}

	// receive remaining blocks
	for i, p := range peers {
		session.ReceiveBlockFrom(p, blks[testutil.IndexOf(blks, newBlockReqs[i].cids[0])])
		receivedBlock := <-getBlocksCh
		receivedBlocks = append(receivedBlocks, receivedBlock)
		cancelBlock := <-cancelReqs
		newCancelReqs = append(newCancelReqs, cancelBlock)
	}

	if len(receivedBlocks) != len(blks) {
		t.Fatal("did not receive enough blocks")
	}
	for _, block := range receivedBlocks {
		if !testutil.ContainsBlock(blks, block) {
			t.Fatal("received incorrect block")
		}
	}
}

func TestSessionFindMorePeers(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	wantReqs := make(chan wantReq, 1)
	cancelReqs := make(chan wantReq, 1)
	fwm := &fakeWantManager{wantReqs, cancelReqs}
	fpm := &fakePeerManager{}
	id := testutil.GenerateSessionID()
	session := New(ctx, id, fwm, fpm)
	session.SetBaseTickDelay(200 * time.Microsecond)
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(activeWantsLimit * 2)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	getBlocksCh, err := session.GetBlocks(ctx, cids)
	if err != nil {
		t.Fatal("error getting blocks")
	}

	// clear the initial block of wants
	<-wantReqs

	// receive a block to trigger a tick reset
	time.Sleep(200 * time.Microsecond)
	p := testutil.GeneratePeers(1)[0]
	session.ReceiveBlockFrom(p, blks[0])
	<-getBlocksCh
	<-wantReqs
	<-cancelReqs

	// wait long enough for a tick to occur
	time.Sleep(20 * time.Millisecond)

	// trigger to find providers should have happened
	fpm.lk.Lock()
	if fpm.findMorePeersRequested != true {
		t.Fatal("should have attempted to find more peers but didn't")
	}
	fpm.lk.Unlock()

	// verify a broadcast was made
	receivedWantReq := <-wantReqs
	if len(receivedWantReq.cids) != activeWantsLimit {
		t.Fatal("did not rebroadcast whole live list")
	}
	if receivedWantReq.peers != nil {
		t.Fatal("did not make a broadcast")
	}
	<-ctx.Done()
}
