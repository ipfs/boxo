package session

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	bsbpm "github.com/ipfs/boxo/bitswap/client/internal/blockpresencemanager"
	notifications "github.com/ipfs/boxo/bitswap/client/internal/notifications"
	bspm "github.com/ipfs/boxo/bitswap/client/internal/peermanager"
	bssim "github.com/ipfs/boxo/bitswap/client/internal/sessioninterestmanager"
	bsspm "github.com/ipfs/boxo/bitswap/client/internal/sessionpeermanager"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	delay "github.com/ipfs/go-ipfs-delay"
	"github.com/ipfs/go-test/random"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

const blockSize = 4

type mockSessionMgr struct {
	lk            sync.Mutex
	sim           *bssim.SessionInterestManager
	removeSession bool
	cancels       []cid.Cid
}

func newMockSessionMgr(sim *bssim.SessionInterestManager) *mockSessionMgr {
	return &mockSessionMgr{
		sim: sim,
	}
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
	msm.sim.RemoveSessionWants(sid, wants)
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

func (fpf *fakeProviderFinder) FindProvidersAsync(ctx context.Context, k cid.Cid, max int) <-chan peer.AddrInfo {
	go func() {
		select {
		case fpf.findMorePeersRequested <- k:
		case <-ctx.Done():
		}
	}()

	return make(chan peer.AddrInfo)
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
func (pm *fakePeerManager) SendWants(context.Context, peer.ID, []cid.Cid, []cid.Cid) bool {
	return true
}

func (pm *fakePeerManager) BroadcastWantHaves(ctx context.Context, cids []cid.Cid) {
	select {
	case pm.wantReqs <- wantReq{cids}:
	case <-ctx.Done():
	}
}
func (pm *fakePeerManager) SendCancels(ctx context.Context, cancels []cid.Cid) {}

func TestSessionGetBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	sim := bssim.New()
	bpm := bsbpm.New()
	notif := notifications.New()
	defer notif.Shutdown()
	id := random.SequenceNext()
	sm := newMockSessionMgr(sim)
	session := New(ctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, time.Second, delay.Fixed(time.Minute), "")
	blks := random.BlocksOfSize(broadcastLiveWantsLimit*2, blockSize)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}

	_, err := session.GetBlocks(ctx, cids)
	require.NoError(t, err, "error getting blocks")

	// Wait for initial want request
	receivedWantReq := <-fpm.wantReqs

	// Should have registered session's interest in blocks
	intSes := sim.FilterSessionInterested(id, cids)
	require.ElementsMatch(t, intSes[0], cids, "did not register session interest in blocks")

	// Should have sent out broadcast request for wants
	require.Len(t, receivedWantReq.cids, broadcastLiveWantsLimit, "did not enqueue correct initial number of wants")

	// Simulate receiving HAVEs from several peers
	peers := random.Peers(5)
	for i, p := range peers {
		blkIndex := slices.IndexFunc(blks, func(blk blocks.Block) bool {
			return blk.Cid() == receivedWantReq.cids[i]
		})
		blk := blks[blkIndex]
		session.ReceiveFrom(p, []cid.Cid{}, []cid.Cid{blk.Cid()}, []cid.Cid{})
	}

	time.Sleep(10 * time.Millisecond)

	// Verify new peers were recorded
	require.ElementsMatch(t, fspm.Peers(), peers, "peers not recorded by the peer manager")

	// Verify session still wants received blocks
	_, unwanted := sim.SplitWantedUnwanted(blks)
	require.Empty(t, unwanted, "all blocks should still be wanted")

	// Simulate receiving DONT_HAVE for a CID
	session.ReceiveFrom(peers[0], []cid.Cid{}, []cid.Cid{}, []cid.Cid{blks[0].Cid()})

	time.Sleep(10 * time.Millisecond)

	// Verify session still wants received blocks
	_, unwanted = sim.SplitWantedUnwanted(blks)
	require.Empty(t, unwanted, "all blocks should still be wanted")

	// Simulate receiving block for a CID
	session.ReceiveFrom(peers[1], []cid.Cid{blks[0].Cid()}, []cid.Cid{}, []cid.Cid{})

	time.Sleep(10 * time.Millisecond)

	// Verify session no longer wants received block
	wanted, unwanted := sim.SplitWantedUnwanted(blks)
	require.Len(t, unwanted, 1)
	require.True(t, unwanted[0].Cid().Equals(blks[0].Cid()), "session wants block that has already been received")
	require.Len(t, wanted, len(blks)-1, "session wants incorrect number of blocks")

	// Shut down session
	cancel()

	time.Sleep(10 * time.Millisecond)

	// Verify session was removed
	require.True(t, sm.removeSessionCalled(), "expected session to be removed")
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
	id := random.SequenceNext()
	sm := newMockSessionMgr(sim)
	session := New(ctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, time.Second, delay.Fixed(time.Minute), "")
	session.SetBaseTickDelay(200 * time.Microsecond)
	blks := random.BlocksOfSize(broadcastLiveWantsLimit*2, blockSize)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	_, err := session.GetBlocks(ctx, cids)
	require.NoError(t, err, "error getting blocks")

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
	p := random.Peers(1)[0]

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
			require.False(t, c.Equals(cids[0]), "should not broadcast block that was already received")
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
	id := random.SequenceNext()
	sm := newMockSessionMgr(sim)
	session := New(ctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, time.Second, delay.Fixed(time.Minute), "")
	blks := random.BlocksOfSize(broadcastLiveWantsLimit+5, blockSize)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	_, err := session.GetBlocks(ctx, cids)
	require.NoError(t, err, "error getting blocks")

	// Wait for initial want request
	receivedWantReq := <-fpm.wantReqs

	// Should have sent out broadcast request for wants
	require.Len(t, receivedWantReq.cids, broadcastLiveWantsLimit, "did not enqueue correct initial number of wants")

	// Signal that all peers have send DONT_HAVE for two of the wants
	session.onPeersExhausted(cids[len(cids)-2:])

	// Wait for want request
	receivedWantReq = <-fpm.wantReqs

	// Should have sent out broadcast request for wants
	require.Len(t, receivedWantReq.cids, 2, "did not enqueue correct initial number of wants")
}

func TestSessionFailingToGetFirstBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	sim := bssim.New()
	bpm := bsbpm.New()
	notif := notifications.New()
	defer notif.Shutdown()
	id := random.SequenceNext()
	sm := newMockSessionMgr(sim)
	session := New(ctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, 10*time.Millisecond, delay.Fixed(100*time.Millisecond), "")
	blks := random.BlocksOfSize(4, blockSize)
	var cids []cid.Cid
	for _, block := range blks {
		cids = append(cids, block.Cid())
	}
	_, err := session.GetBlocks(ctx, cids)
	require.NoError(t, err, "error getting blocks")

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
		if !slices.ContainsFunc(blks, func(blk blocks.Block) bool {
			return blk.Cid() == k
		}) {
			t.Fatal("did not rebroadcast an active want")
		}
	case <-ctx.Done():
		t.Fatal("Did not find more peers")
	}

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
	select {
	case receivedWantReq := <-fpm.wantReqs:
		if len(receivedWantReq.cids) < len(cids) {
			t.Fatal("did not rebroadcast whole live list")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
	}

	// Wait for another broadcast to occur
	select {
	case receivedWantReq := <-fpm.wantReqs:
		if len(receivedWantReq.cids) < len(cids) {
			t.Fatal("did not rebroadcast whole live list")
		}
	case <-ctx.Done():
		t.Fatal("Never rebroadcast want list")
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
		if !slices.ContainsFunc(blks, func(blk blocks.Block) bool {
			return blk.Cid() == k
		}) {
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
	id := random.SequenceNext()
	sm := newMockSessionMgr(sim)

	// Create a new session with its own context
	sessctx, sesscancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	session := New(sessctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, time.Second, delay.Fixed(time.Minute), "")

	timerCtx, timerCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer timerCancel()

	// Request a block with a new context
	blks := random.BlocksOfSize(1, blockSize)
	getctx, getcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer getcancel()

	getBlocksCh, err := session.GetBlocks(getctx, []cid.Cid{blks[0].Cid()})
	require.NoError(t, err, "error getting blocks")

	// Cancel the session context
	sesscancel()

	// Expect the GetBlocks() channel to be closed
	select {
	case _, ok := <-getBlocksCh:
		require.False(t, ok, "expected channel to be closed but was not closed")
	case <-timerCtx.Done():
		t.Fatal("expected channel to be closed before timeout")
	}

	time.Sleep(10 * time.Millisecond)

	// Expect RemoveSession to be called
	require.True(t, sm.removeSessionCalled(), "expected onShutdown to be called")
}

func TestSessionOnShutdownCalled(t *testing.T) {
	fpm := newFakePeerManager()
	fspm := newFakeSessionPeerManager()
	fpf := newFakeProviderFinder()
	sim := bssim.New()
	bpm := bsbpm.New()
	notif := notifications.New()
	defer notif.Shutdown()
	id := random.SequenceNext()
	sm := newMockSessionMgr(sim)

	// Create a new session with its own context
	sessctx, sesscancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer sesscancel()
	session := New(sessctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, time.Second, delay.Fixed(time.Minute), "")

	// Shutdown the session
	session.Shutdown()

	time.Sleep(10 * time.Millisecond)

	// Expect RemoveSession to be called
	require.True(t, sm.removeSessionCalled(), "expected onShutdown to be called")
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
	id := random.SequenceNext()
	sm := newMockSessionMgr(sim)
	session := New(ctx, sm, id, fspm, fpf, sim, fpm, bpm, notif, time.Second, delay.Fixed(time.Minute), "")
	blks := random.BlocksOfSize(2, blockSize)
	cids := []cid.Cid{blks[0].Cid(), blks[1].Cid()}

	_, err := session.GetBlocks(ctx, cids)
	require.NoError(t, err, "error getting blocks")

	// Wait for initial want request
	<-fpm.wantReqs

	// Shut down session
	cancelCtx()

	// Simulate receiving block for a CID
	peer := random.Peers(1)[0]
	session.ReceiveFrom(peer, []cid.Cid{blks[0].Cid()}, []cid.Cid{}, []cid.Cid{})

	time.Sleep(5 * time.Millisecond)

	// If we don't get a panic then the test is considered passing
}
