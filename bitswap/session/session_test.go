package session

import (
	"context"
	"fmt"
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
	cids     []cid.Cid
	peers    []peer.ID
	isCancel bool
}

type fakeWantManager struct {
	lk       sync.RWMutex
	wantReqs []wantReq
}

func (fwm *fakeWantManager) WantBlocks(ctx context.Context, cids []cid.Cid, peers []peer.ID, ses uint64) {
	fwm.lk.Lock()
	fwm.wantReqs = append(fwm.wantReqs, wantReq{cids, peers, false})
	fwm.lk.Unlock()
}

func (fwm *fakeWantManager) CancelWants(ctx context.Context, cids []cid.Cid, peers []peer.ID, ses uint64) {
	fwm.lk.Lock()
	fwm.wantReqs = append(fwm.wantReqs, wantReq{cids, peers, true})
	fwm.lk.Unlock()
}

type fakePeerManager struct {
	peers                  []peer.ID
	findMorePeersRequested bool
}

func (fpm *fakePeerManager) FindMorePeers(context.Context, cid.Cid) {
	fpm.findMorePeersRequested = true
}

func (fpm *fakePeerManager) GetOptimizedPeers() []peer.ID {
	return fpm.peers
}

func (fpm *fakePeerManager) RecordPeerRequests([]peer.ID, []cid.Cid) {}
func (fpm *fakePeerManager) RecordPeerResponse(p peer.ID, c cid.Cid) {
	fpm.peers = append(fpm.peers, p)
}

func TestSessionGetBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	fwm := &fakeWantManager{}
	fpm := &fakePeerManager{}
	id := testutil.GenerateSessionID()
	session := New(ctx, id, fwm, fpm)
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(activeWantsLimit * 2)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	var receivedBlocks []blocks.Block
	getBlocksCh, err := session.GetBlocks(ctx, cids)
	go func() {
		for block := range getBlocksCh {
			receivedBlocks = append(receivedBlocks, block)
		}
	}()
	if err != nil {
		t.Fatal("error getting blocks")
	}

	// check initial want request
	time.Sleep(3 * time.Millisecond)
	if len(fwm.wantReqs) != 1 {
		t.Fatal("failed to enqueue wants")
	}
	fwm.lk.Lock()
	receivedWantReq := fwm.wantReqs[0]
	if len(receivedWantReq.cids) != activeWantsLimit {
		t.Fatal("did not enqueue correct initial number of wants")
	}
	if receivedWantReq.peers != nil {
		t.Fatal("first want request should be a broadcast")
	}

	fwm.wantReqs = nil
	fwm.lk.Unlock()

	// now receive the first set of blocks
	peers := testutil.GeneratePeers(activeWantsLimit)
	for i, p := range peers {
		session.ReceiveBlockFrom(p, blks[i])
	}
	time.Sleep(3 * time.Millisecond)

	// verify new peers were recorded
	if len(fpm.peers) != activeWantsLimit {
		t.Fatal("received blocks not recorded by the peer manager")
	}
	for _, p := range fpm.peers {
		if !testutil.ContainsPeer(peers, p) {
			t.Fatal("incorrect peer recorded to peer manager")
		}
	}

	// look at new interactions with want manager
	var cancelReqs []wantReq
	var newBlockReqs []wantReq

	fwm.lk.Lock()
	for _, w := range fwm.wantReqs {
		if w.isCancel {
			cancelReqs = append(cancelReqs, w)
		} else {
			newBlockReqs = append(newBlockReqs, w)
		}
	}
	// should have cancelled each received block
	if len(cancelReqs) != activeWantsLimit {
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
	fwm.lk.Unlock()

	// full new round of cids should be requested
	if totalEnqueued != activeWantsLimit {
		t.Fatal("new blocks were not requested")
	}

	// receive remaining blocks
	for i, p := range peers {
		session.ReceiveBlockFrom(p, blks[i+activeWantsLimit])
	}

	// wait for everything to wrap up
	<-ctx.Done()

	// check that we got everything
	fmt.Printf("%d\n", len(receivedBlocks))

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
	fwm := &fakeWantManager{}
	fpm := &fakePeerManager{}
	id := testutil.GenerateSessionID()
	session := New(ctx, id, fwm, fpm)
	session.SetBaseTickDelay(1 * time.Millisecond)
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(activeWantsLimit * 2)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	var receivedBlocks []blocks.Block
	getBlocksCh, err := session.GetBlocks(ctx, cids)
	go func() {
		for block := range getBlocksCh {
			receivedBlocks = append(receivedBlocks, block)
		}
	}()
	if err != nil {
		t.Fatal("error getting blocks")
	}

	// receive a block to trigger a tick reset
	time.Sleep(1 * time.Millisecond)
	p := testutil.GeneratePeers(1)[0]
	session.ReceiveBlockFrom(p, blks[0])

	// wait then clear the want list
	time.Sleep(1 * time.Millisecond)
	fwm.lk.Lock()
	fwm.wantReqs = nil
	fwm.lk.Unlock()

	// wait long enough for a tick to occur
	// baseTickDelay + 3 * latency = 4ms
	time.Sleep(6 * time.Millisecond)

	// trigger to find providers should have happened
	if fpm.findMorePeersRequested != true {
		t.Fatal("should have attempted to find more peers but didn't")
	}

	// verify a broadcast was made
	fwm.lk.Lock()
	if len(fwm.wantReqs) != 1 {
		t.Fatal("did not make a new broadcast")
	}
	receivedWantReq := fwm.wantReqs[0]
	if len(receivedWantReq.cids) != activeWantsLimit {
		t.Fatal("did not rebroadcast whole live list")
	}
	if receivedWantReq.peers != nil {
		t.Fatal("did not make a broadcast")
	}
	fwm.wantReqs = nil
	fwm.lk.Unlock()
}
