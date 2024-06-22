package session

import (
	"context"
	"sync"
	"testing"
	"time"

	bsbpm "github.com/ipfs/boxo/bitswap/client/internal/blockpresencemanager"
	notifications "github.com/ipfs/boxo/bitswap/client/internal/notifications"
	bspm "github.com/ipfs/boxo/bitswap/client/internal/peermanager"
	bssim "github.com/ipfs/boxo/bitswap/client/internal/sessioninterestmanager"
	bsspm "github.com/ipfs/boxo/bitswap/client/internal/sessionpeermanager"
	"github.com/ipfs/boxo/bitswap/internal/testutil"
	"github.com/ipfs/boxo/internal/test"
	cid "github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	delay "github.com/ipfs/go-ipfs-delay"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

type mockSessionMgr struct {
	lk            sync.Mutex
	removeSession bool
	cancels       []cid.Cid
}

func newMockSessionMgr() *mockSessionMgr {
	return &mockSessionMgr{}
}

func (msm *mockSessionMgr) removeSessionCalled() bool {
	msm.lk.Lock()
	defer msm.lk.Unlock()
	return msm.removeSession
}

func (msm *mockSessionMgr) cancelled() []cid.Cid {
	msm.lk.Lock()
	defer msm.lk.Unlock()
	return msm.cancels
}

func (msm *mockSessionMgr) RemoveSession(sesid uint64) {
	msm.lk.Lock()
	defer msm.lk.Unlock()
	msm.removeSession = true
}

func (msm *mockSessionMgr) CancelSessionWants(sid uint64, wants []cid.Cid) {
	msm.lk.Lock()
	defer msm.lk.Unlock()
	msm.cancels = append(msm.cancels, wants...)
}

func newFakeSessionPeerManager() *bsspm.SessionPeerManager {
	return bsspm.New(1, newFakePeerTagger())
}

func newFakePeerTagger() *fakePeerTagger {
	return &fakePeerTagger{
		protectedPeers: make(map[peer.ID]map[string]struct{}),
	}
}

type fakePeerTagger struct {
	lk             sync.Mutex
	protectedPeers map[peer.ID]map[string]struct{}
}

func (fpt *fakePeerTagger) TagPeer(p peer.ID, tag string, val int) {}
func (fpt *fakePeerTagger) UntagPeer(p peer.ID, tag string)        {}

func (fpt *fakePeerTagger) Protect(p peer.ID, tag string) {
	fpt.lk.Lock()
	defer fpt.lk.Unlock()

	tags, ok := fpt.protectedPeers[p]
	if !ok {
		tags = make(map[string]struct{})
		fpt.protectedPeers[p] = tags
	}
	tags[tag] = struct{}{}
}

func (fpt *fakePeerTagger) Unprotect(p peer.ID, tag string) bool {
	fpt.lk.Lock()
	defer fpt.lk.Unlock()

	if tags, ok := fpt.protectedPeers[p]; ok {
		delete(tags, tag)
		return len(tags) > 0
	}

	return false
}

func (fpt *fakePeerTagger) isProtected(p peer.ID) bool {
	fpt.lk.Lock()
	defer fpt.lk.Unlock()

	return len(fpt.protectedPeers[p]) > 0
}

type fakeProviderFinder struct {
	findMorePeersRequested chan cid.Cid
}

func newFakeProviderFinder() *fakeProviderFinder {
	return &fakeProviderFinder{
		findMorePeersRequested: make(chan cid.Cid, 1),
	}
}

func (fpf *fakeProviderFinder) FindProvidersAsync(ctx context.Context, k cid.Cid) <-chan peer.ID {
	go func() {
		select {
		case fpf.findMorePeersRequested <- k:
		case <-ctx.Done():
		}
	}()

	return make(chan peer.ID)
}

type wantReq struct {
	cids []cid.Cid
}

type fakePeerManager struct {
	wantReqs chan wantReq
}

func newFakePeerManager() *fakePeerManager {
	return &fakePeerManager{
		wantReqs: make(chan wantReq, 1),
	}
}

func (pm *fakePeerManager) RegisterSession(peer.ID, bspm.Session) {}
func (pm *fakePeerManager) UnregisterSession(uint64)              {}
func (pm *fakePeerManager) SendWants(context.Context, peer.ID, []cid.Cid, []cid.Cid) error {
	return nil
}
func (pm *fakePeerManager) BroadcastWantHaves(ctx context.Context, cids []cid.Cid) {
	select {
	case pm.wantReqs <- wantReq{cids}:
	case <-ctx.Done():
	}
}
func (pm *fakePeerManager) SendCancels(ctx context.Context, cancels []cid.Cid) {}

func TestSessionGetBlocks(t *testing.T) {
	test.Flaky(t)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	sim := bssim.New()
	bpm := bsbpm.New()
	notif := notifications.New()
	defer notif.Shutdown()
	id := testutil.GenerateSessionID()
	sm := newMockSessionMgr()
	session := New(ctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, time.Second, delay.Fixed(time.Minute), "")
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(broadcastLiveWantsLimit * 2)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}

	_, err := session.GetBlocks(ctx, cids)
	if err != nil {
		t.Fatal("error getting blocks")
	}

	// Wait for initial want request
	receivedWantReq := <-fpm.wantReqs

	// Should have registered session's interest in blocks
	intSes := sim.FilterSessionInterested(id, cids)
	if !testutil.MatchKeysIgnoreOrder(intSes[0], cids) {
		t.Fatal("did not register session interest in blocks")
	}

	// Should have sent out broadcast request for wants
	if len(receivedWantReq.cids) != broadcastLiveWantsLimit {
		t.Fatal("did not enqueue correct initial number of wants")
	}

	// Simulate receiving HAVEs from several peers
	peers := testutil.GeneratePeers(5)
	for i, p := range peers {
		blk := blks[testutil.IndexOf(blks, receivedWantReq.cids[i])]
		session.ReceiveFrom(p, []cid.Cid{}, []cid.Cid{blk.Cid()}, []cid.Cid{})
	}

	time.Sleep(10 * time.Millisecond)

	// Verify new peers were recorded
	if !testutil.MatchPeersIgnoreOrder(fspm.Peers(), peers) {
		t.Fatal("peers not recorded by the peer manager")
	}

	// Verify session still wants received blocks
	_, unwanted := sim.SplitWantedUnwanted(blks)
	if len(unwanted) > 0 {
		t.Fatal("all blocks should still be wanted")
	}

	// Simulate receiving DONT_HAVE for a CID
	session.ReceiveFrom(peers[0], []cid.Cid{}, []cid.Cid{}, []cid.Cid{blks[0].Cid()})

	time.Sleep(10 * time.Millisecond)

	// Verify session still wants received blocks
	_, unwanted = sim.SplitWantedUnwanted(blks)
	if len(unwanted) > 0 {
		t.Fatal("all blocks should still be wanted")
	}

	// Simulate receiving block for a CID
	session.ReceiveFrom(peers[1], []cid.Cid{blks[0].Cid()}, []cid.Cid{}, []cid.Cid{})

	time.Sleep(10 * time.Millisecond)

	// Verify session no longer wants received block
	wanted, unwanted := sim.SplitWantedUnwanted(blks)
	if len(unwanted) != 1 || !unwanted[0].Cid().Equals(blks[0].Cid()) {
		t.Fatal("session wants block that has already been received")
	}
	if len(wanted) != len(blks)-1 {
		t.Fatal("session wants incorrect number of blocks")
	}

	// Shut down session
	cancel()

	time.Sleep(10 * time.Millisecond)

	// Verify session was removed
	if !sm.removeSessionCalled() {
		t.Fatal("expected session to be removed")
	}
}

func TestSessionFindMorePeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	sim := bssim.New()
	bpm := bsbpm.New()
	notif := notifications.New()
	defer notif.Shutdown()
	id := testutil.GenerateSessionID()
	sm := newMockSessionMgr()
	session := New(ctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, time.Second, delay.Fixed(time.Minute), "")
	session.SetBaseTickDelay(200 * time.Microsecond)
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(broadcastLiveWantsLimit * 2)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	_, err := session.GetBlocks(ctx, cids)
	if err != nil {
		t.Fatal("error getting blocks")
	}

	// The session should initially broadcast want-haves
	select {
	case <-fpm.wantReqs:
	case <-ctx.Done():
		t.Fatal("Did not make first want request ")
	}

	// receive a block to trigger a tick reset
	time.Sleep(20 * time.Millisecond) // need to make sure some latency registers
	// or there will be no tick set -- time precision on Windows in go is in the
	// millisecond range
	p := testutil.GeneratePeers(1)[0]

	blk := blks[0]
	session.ReceiveFrom(p, []cid.Cid{blk.Cid()}, []cid.Cid{}, []cid.Cid{})

	// The session should now time out waiting for a response and broadcast
	// want-haves again
	select {
	case <-fpm.wantReqs:
	case <-ctx.Done():
		t.Fatal("Did not make second want request ")
	}

	// The session should keep broadcasting periodically until it receives a response
	select {
	case receivedWantReq := <-fpm.wantReqs:
		if len(receivedWantReq.cids) != broadcastLiveWantsLimit {
			t.Fatal("did not rebroadcast whole live list")
		}
		// Make sure the first block is not included because it has already
		// been received
		for _, c := range receivedWantReq.cids {
			if c.Equals(cids[0]) {
				t.Fatal("should not braodcast block that was already received")
			}
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}

	// The session should eventually try to find more peers
	select {
	case <-fpf.findMorePeersRequested:
	case <-ctx.Done():
		t.Fatal("Did not find more peers")
	}
}

func TestSessionOnPeersExhausted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()

	sim := bssim.New()
	bpm := bsbpm.New()
	notif := notifications.New()
	defer notif.Shutdown()
	id := testutil.GenerateSessionID()
	sm := newMockSessionMgr()
	session := New(ctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, time.Second, delay.Fixed(time.Minute), "")
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(broadcastLiveWantsLimit + 5)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	_, err := session.GetBlocks(ctx, cids)
	if err != nil {
		t.Fatal("error getting blocks")
	}

	// Wait for initial want request
	receivedWantReq := <-fpm.wantReqs

	// Should have sent out broadcast request for wants
	if len(receivedWantReq.cids) != broadcastLiveWantsLimit {
		t.Fatal("did not enqueue correct initial number of wants")
	}

	// Signal that all peers have send DONT_HAVE for two of the wants
	session.onPeersExhausted(cids[len(cids)-2:])

	// Wait for want request
	receivedWantReq = <-fpm.wantReqs

	// Should have sent out broadcast request for wants
	if len(receivedWantReq.cids) != 2 {
		t.Fatal("did not enqueue correct initial number of wants")
	}
}

func TestSessionFailingToGetFirstBlock(t *testing.T) {
	test.Flaky(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	sim := bssim.New()
	bpm := bsbpm.New()
	notif := notifications.New()
	defer notif.Shutdown()
	id := testutil.GenerateSessionID()
	sm := newMockSessionMgr()
	session := New(ctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, 10*time.Millisecond, delay.Fixed(100*time.Millisecond), "")
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

	// The session should initially broadcast want-haves
	select {
	case <-fpm.wantReqs:
	case <-ctx.Done():
		t.Fatal("Did not make first want request ")
	}

	// Verify a broadcast was made
	select {
	case receivedWantReq := <-fpm.wantReqs:
		if len(receivedWantReq.cids) < len(cids) {
			t.Fatal("did not rebroadcast whole live list")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}

	// Wait for a request to find more peers to occur
	select {
	case k := <-fpf.findMorePeersRequested:
		if testutil.IndexOf(blks, k) == -1 {
			t.Fatal("did not rebroadcast an active want")
		}
	case <-ctx.Done():
		t.Fatal("Did not find more peers")
	}
	firstTickLength := time.Since(startTick)

	// Wait for another broadcast to occur
	select {
	case receivedWantReq := <-fpm.wantReqs:
		if len(receivedWantReq.cids) < len(cids) {
			t.Fatal("did not rebroadcast whole live list")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}

	// Wait for another broadcast to occur
	startTick = time.Now()
	select {
	case receivedWantReq := <-fpm.wantReqs:
		if len(receivedWantReq.cids) < len(cids) {
			t.Fatal("did not rebroadcast whole live list")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}

	// Tick should take longer
	consecutiveTickLength := time.Since(startTick)
	if firstTickLength > consecutiveTickLength {
		t.Fatal("Should have increased tick length after first consecutive tick")
	}

	// Wait for another broadcast to occur
	startTick = time.Now()
	select {
	case receivedWantReq := <-fpm.wantReqs:
		if len(receivedWantReq.cids) < len(cids) {
			t.Fatal("did not rebroadcast whole live list")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}

	// Tick should take longer
	secondConsecutiveTickLength := time.Since(startTick)
	if consecutiveTickLength > secondConsecutiveTickLength {
		t.Fatal("Should have increased tick length after first consecutive tick")
	}

	// Should not have tried to find peers on consecutive ticks
	select {
	case <-fpf.findMorePeersRequested:
		t.Fatal("Should not have tried to find peers on consecutive ticks")
	default:
	}

	// Wait for rebroadcast to occur
	select {
	case k := <-fpf.findMorePeersRequested:
		if testutil.IndexOf(blks, k) == -1 {
			t.Fatal("did not rebroadcast an active want")
		}
	case <-ctx.Done():
		t.Fatal("Did not rebroadcast to find more peers")
	}
}

func TestSessionCtxCancelClosesGetBlocksChannel(t *testing.T) {
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	sim := bssim.New()
	bpm := bsbpm.New()
	notif := notifications.New()
	defer notif.Shutdown()
	id := testutil.GenerateSessionID()
	sm := newMockSessionMgr()

	// Create a new session with its own context
	sessctx, sesscancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	session := New(sessctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, time.Second, delay.Fixed(time.Minute), "")

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

	time.Sleep(10 * time.Millisecond)

	// Expect RemoveSession to be called
	if !sm.removeSessionCalled() {
		t.Fatal("expected onShutdown to be called")
	}
}

func TestSessionOnShutdownCalled(t *testing.T) {
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	sim := bssim.New()
	bpm := bsbpm.New()
	notif := notifications.New()
	defer notif.Shutdown()
	id := testutil.GenerateSessionID()
	sm := newMockSessionMgr()

	// Create a new session with its own context
	sessctx, sesscancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer sesscancel()
	session := New(sessctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, time.Second, delay.Fixed(time.Minute), "")

	// Shutdown the session
	session.Shutdown()

	time.Sleep(10 * time.Millisecond)

	// Expect RemoveSession to be called
	if !sm.removeSessionCalled() {
		t.Fatal("expected onShutdown to be called")
	}
}

func TestSessionReceiveMessageAfterCtxCancel(t *testing.T) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), 20*time.Millisecond)
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()

	sim := bssim.New()
	bpm := bsbpm.New()
	notif := notifications.New()
	defer notif.Shutdown()
	id := testutil.GenerateSessionID()
	sm := newMockSessionMgr()
	session := New(ctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, time.Second, delay.Fixed(time.Minute), "")
	blockGenerator := blocksutil.NewBlockGenerator()
	blks := blockGenerator.Blocks(2)
	cids := []cid.Cid{blks[0].Cid(), blks[1].Cid()}

	_, err := session.GetBlocks(ctx, cids)
	if err != nil {
		t.Fatal("error getting blocks")
	}

	// Wait for initial want request
	<-fpm.wantReqs

	// Shut down session
	cancelCtx()

	// Simulate receiving block for a CID
	peer := testutil.GeneratePeers(1)[0]
	session.ReceiveFrom(peer, []cid.Cid{blks[0].Cid()}, []cid.Cid{}, []cid.Cid{})

	time.Sleep(5 * time.Millisecond)

	// If we don't get a panic then the test is considered passing
}
