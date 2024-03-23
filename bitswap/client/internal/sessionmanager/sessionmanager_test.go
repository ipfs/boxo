package sessionmanager

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	bsbpm "github.com/ipfs/boxo/bitswap/client/internal/blockpresencemanager"
	notifications "github.com/ipfs/boxo/bitswap/client/internal/notifications"
	bspm "github.com/ipfs/boxo/bitswap/client/internal/peermanager"
	bssession "github.com/ipfs/boxo/bitswap/client/internal/session"
	bssim "github.com/ipfs/boxo/bitswap/client/internal/sessioninterestmanager"
	"github.com/ipfs/boxo/bitswap/internal/testutil"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	delay "github.com/ipfs/go-ipfs-delay"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

type fakeSession struct {
	ks         []cid.Cid
	wantBlocks []cid.Cid
	wantHaves  []cid.Cid
	id         uint64
	pm         *fakeSesPeerManager
	sm         bssession.SessionManager
	notif      notifications.PubSub
}

func (*fakeSession) GetBlock(context.Context, cid.Cid) (blocks.Block, error) {
	return nil, nil
}

func (*fakeSession) GetBlocks(context.Context, []cid.Cid) (<-chan blocks.Block, error) {
	return nil, nil
}

func (fs *fakeSession) ID() uint64 {
	return fs.id
}

func (fs *fakeSession) ReceiveFrom(p peer.ID, ks []cid.Cid, wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	fs.ks = append(fs.ks, ks...)
	fs.wantBlocks = append(fs.wantBlocks, wantBlocks...)
	fs.wantHaves = append(fs.wantHaves, wantHaves...)
}

func (fs *fakeSession) Shutdown() {
	fs.sm.RemoveSession(fs.id)
}

type fakeSesPeerManager struct{}

func (*fakeSesPeerManager) Peers() []peer.ID          { return nil }
func (*fakeSesPeerManager) PeersDiscovered() bool     { return false }
func (*fakeSesPeerManager) Shutdown()                 {}
func (*fakeSesPeerManager) AddPeer(peer.ID) bool      { return false }
func (*fakeSesPeerManager) RemovePeer(peer.ID) bool   { return false }
func (*fakeSesPeerManager) HasPeers() bool            { return false }
func (*fakeSesPeerManager) ProtectConnection(peer.ID) {}

type fakePeerManager struct {
	lk      sync.Mutex
	cancels []cid.Cid
}

func (*fakePeerManager) RegisterSession(peer.ID, bspm.Session)                          {}
func (*fakePeerManager) UnregisterSession(uint64)                                       {}
func (*fakePeerManager) SendWants(context.Context, peer.ID, []cid.Cid, []cid.Cid) error { return nil }
func (*fakePeerManager) BroadcastWantHaves(context.Context, []cid.Cid)                  {}
func (fpm *fakePeerManager) SendCancels(ctx context.Context, cancels []cid.Cid) {
	fpm.lk.Lock()
	defer fpm.lk.Unlock()
	fpm.cancels = append(fpm.cancels, cancels...)
}

func (fpm *fakePeerManager) cancelled() []cid.Cid {
	fpm.lk.Lock()
	defer fpm.lk.Unlock()
	return fpm.cancels
}

func sessionFactory(ctx context.Context,
	sm bssession.SessionManager,
	id uint64,
	sprm bssession.SessionPeerManager,
	sim *bssim.SessionInterestManager,
	pm bssession.PeerManager,
	bpm *bsbpm.BlockPresenceManager,
	notif notifications.PubSub,
	provSearchDelay time.Duration,
	rebroadcastDelay delay.D,
	self peer.ID,
) Session {
	fs := &fakeSession{
		id:    id,
		pm:    sprm.(*fakeSesPeerManager),
		sm:    sm,
		notif: notif,
	}
	go func() {
		<-ctx.Done()
		sm.RemoveSession(fs.id)
	}()
	return fs
}

func peerManagerFactory(ctx context.Context, id uint64) bssession.SessionPeerManager {
	return &fakeSesPeerManager{}
}

func TestReceiveFrom(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	notif := notifications.New()
	defer notif.Shutdown()
	sim := bssim.New()
	bpm := bsbpm.New()
	pm := &fakePeerManager{}
	sm := New(ctx, sessionFactory, sim, peerManagerFactory, bpm, pm, notif, "")

	p := peer.ID(strconv.Itoa(123))
	block := blocks.NewBlock([]byte("block"))

	firstSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	secondSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	thirdSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)

	sim.RecordSessionInterest(firstSession.ID(), []cid.Cid{block.Cid()})
	sim.RecordSessionInterest(thirdSession.ID(), []cid.Cid{block.Cid()})

	sm.ReceiveFrom(ctx, p, []cid.Cid{block.Cid()}, []cid.Cid{}, []cid.Cid{})
	if len(firstSession.ks) == 0 ||
		len(secondSession.ks) > 0 ||
		len(thirdSession.ks) == 0 {
		t.Fatal("should have received blocks but didn't")
	}

	sm.ReceiveFrom(ctx, p, []cid.Cid{}, []cid.Cid{block.Cid()}, []cid.Cid{})
	if len(firstSession.wantBlocks) == 0 ||
		len(secondSession.wantBlocks) > 0 ||
		len(thirdSession.wantBlocks) == 0 {
		t.Fatal("should have received want-blocks but didn't")
	}

	sm.ReceiveFrom(ctx, p, []cid.Cid{}, []cid.Cid{}, []cid.Cid{block.Cid()})
	if len(firstSession.wantHaves) == 0 ||
		len(secondSession.wantHaves) > 0 ||
		len(thirdSession.wantHaves) == 0 {
		t.Fatal("should have received want-haves but didn't")
	}

	if len(pm.cancelled()) != 1 {
		t.Fatal("should have sent cancel for received blocks")
	}
}

func TestReceiveBlocksWhenManagerShutdown(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	notif := notifications.New()
	defer notif.Shutdown()
	sim := bssim.New()
	bpm := bsbpm.New()
	pm := &fakePeerManager{}
	sm := New(ctx, sessionFactory, sim, peerManagerFactory, bpm, pm, notif, "")

	p := peer.ID(strconv.Itoa(123))
	block := blocks.NewBlock([]byte("block"))

	firstSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	secondSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	thirdSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)

	sim.RecordSessionInterest(firstSession.ID(), []cid.Cid{block.Cid()})
	sim.RecordSessionInterest(secondSession.ID(), []cid.Cid{block.Cid()})
	sim.RecordSessionInterest(thirdSession.ID(), []cid.Cid{block.Cid()})

	sm.Shutdown()

	// wait for sessions to get removed
	time.Sleep(10 * time.Millisecond)

	sm.ReceiveFrom(ctx, p, []cid.Cid{block.Cid()}, []cid.Cid{}, []cid.Cid{})
	if len(firstSession.ks) > 0 ||
		len(secondSession.ks) > 0 ||
		len(thirdSession.ks) > 0 {
		t.Fatal("received blocks for sessions after manager is shutdown")
	}
}

func TestReceiveBlocksWhenSessionContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	notif := notifications.New()
	defer notif.Shutdown()
	sim := bssim.New()
	bpm := bsbpm.New()
	pm := &fakePeerManager{}
	sm := New(ctx, sessionFactory, sim, peerManagerFactory, bpm, pm, notif, "")

	p := peer.ID(strconv.Itoa(123))
	block := blocks.NewBlock([]byte("block"))

	firstSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	sessionCtx, sessionCancel := context.WithCancel(ctx)
	secondSession := sm.NewSession(sessionCtx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	thirdSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)

	sim.RecordSessionInterest(firstSession.ID(), []cid.Cid{block.Cid()})
	sim.RecordSessionInterest(secondSession.ID(), []cid.Cid{block.Cid()})
	sim.RecordSessionInterest(thirdSession.ID(), []cid.Cid{block.Cid()})

	sessionCancel()

	// wait for sessions to get removed
	time.Sleep(10 * time.Millisecond)

	sm.ReceiveFrom(ctx, p, []cid.Cid{block.Cid()}, []cid.Cid{}, []cid.Cid{})
	if len(firstSession.ks) == 0 ||
		len(secondSession.ks) > 0 ||
		len(thirdSession.ks) == 0 {
		t.Fatal("received blocks for sessions that are canceled")
	}
}

func TestShutdown(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	notif := notifications.New()
	defer notif.Shutdown()
	sim := bssim.New()
	bpm := bsbpm.New()
	pm := &fakePeerManager{}
	sm := New(ctx, sessionFactory, sim, peerManagerFactory, bpm, pm, notif, "")

	p := peer.ID(strconv.Itoa(123))
	block := blocks.NewBlock([]byte("block"))
	cids := []cid.Cid{block.Cid()}
	firstSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	sim.RecordSessionInterest(firstSession.ID(), cids)
	sm.ReceiveFrom(ctx, p, []cid.Cid{}, []cid.Cid{}, cids)

	if !bpm.HasKey(block.Cid()) {
		t.Fatal("expected cid to be added to block presence manager")
	}

	sm.Shutdown()

	// wait for cleanup
	time.Sleep(10 * time.Millisecond)

	if bpm.HasKey(block.Cid()) {
		t.Fatal("expected cid to be removed from block presence manager")
	}
	if !testutil.MatchKeysIgnoreOrder(pm.cancelled(), cids) {
		t.Fatal("expected cancels to be sent")
	}
}
