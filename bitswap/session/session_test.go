package session

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-block-format"

	bssrs "github.com/ipfs/go-bitswap/sessionrequestsplitter"
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
	findMorePeersRequested chan struct{}
}

func (fpm *fakePeerManager) FindMorePeers(ctx context.Context, k cid.Cid) {
	select {
	case fpm.findMorePeersRequested <- struct{}{}:
	case <-ctx.Done():
	}
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

type fakeRequestSplitter struct {
}

func (frs *fakeRequestSplitter) SplitRequest(peers []peer.ID, keys []cid.Cid) []*bssrs.PartialRequest {
	return []*bssrs.PartialRequest{&bssrs.PartialRequest{Peers: peers, Keys: keys}}
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
	id := testutil.GenerateSessionID()
	session := New(ctx, id, fwm, fpm, frs)
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

	// now receive the first set of blocks
	peers := testutil.GeneratePeers(broadcastLiveWantsLimit)
	var newCancelReqs []wantReq
	var newBlockReqs []wantReq
	var receivedBlocks []blocks.Block
	for i, p := range peers {
		session.ReceiveBlockFrom(p, blks[testutil.IndexOf(blks, receivedWantReq.cids[i])])
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
		session.ReceiveBlockFrom(p, blks[testutil.IndexOf(blks, newCidsRequested[i])])
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

	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	wantReqs := make(chan wantReq, 1)
	cancelReqs := make(chan wantReq, 1)
	fwm := &fakeWantManager{wantReqs, cancelReqs}
	fpm := &fakePeerManager{findMorePeersRequested: make(chan struct{}, 1)}
	frs := &fakeRequestSplitter{}
	id := testutil.GenerateSessionID()
	session := New(ctx, id, fwm, fpm, frs)
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
	session.ReceiveBlockFrom(p, blks[0])
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
