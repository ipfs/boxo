package sessionmanager

import (
	"context"
	"testing"
	"time"

	delay "github.com/ipfs/go-ipfs-delay"

	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"
	notifications "github.com/ipfs/go-bitswap/internal/notifications"
	bspm "github.com/ipfs/go-bitswap/internal/peermanager"
	bssession "github.com/ipfs/go-bitswap/internal/session"
	bssim "github.com/ipfs/go-bitswap/internal/sessioninterestmanager"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type fakeSession struct {
	ks         []cid.Cid
	wantBlocks []cid.Cid
	wantHaves  []cid.Cid
	id         uint64
	pm         *fakeSesPeerManager
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

type fakeSesPeerManager struct {
}

func (*fakeSesPeerManager) ReceiveFrom(peer.ID, []cid.Cid, []cid.Cid) bool { return true }
func (*fakeSesPeerManager) Peers() *peer.Set                               { return nil }
func (*fakeSesPeerManager) FindMorePeers(context.Context, cid.Cid)         {}
func (*fakeSesPeerManager) RecordPeerRequests([]peer.ID, []cid.Cid)        {}
func (*fakeSesPeerManager) RecordPeerResponse(peer.ID, []cid.Cid)          {}
func (*fakeSesPeerManager) RecordCancels(c []cid.Cid)                      {}

type fakePeerManager struct {
}

func (*fakePeerManager) RegisterSession(peer.ID, bspm.Session) bool               { return true }
func (*fakePeerManager) UnregisterSession(uint64)                                 {}
func (*fakePeerManager) SendWants(context.Context, peer.ID, []cid.Cid, []cid.Cid) {}

func sessionFactory(ctx context.Context,
	id uint64,
	sprm bssession.SessionPeerManager,
	sim *bssim.SessionInterestManager,
	pm bssession.PeerManager,
	bpm *bsbpm.BlockPresenceManager,
	notif notifications.PubSub,
	provSearchDelay time.Duration,
	rebroadcastDelay delay.D,
	self peer.ID) Session {
	return &fakeSession{
		id:    id,
		pm:    sprm.(*fakeSesPeerManager),
		notif: notif,
	}
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

	p := peer.ID(123)
	block := blocks.NewBlock([]byte("block"))

	firstSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	secondSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	thirdSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)

	sim.RecordSessionInterest(firstSession.ID(), []cid.Cid{block.Cid()})
	sim.RecordSessionInterest(thirdSession.ID(), []cid.Cid{block.Cid()})

	sm.ReceiveFrom(p, []cid.Cid{block.Cid()}, []cid.Cid{}, []cid.Cid{})
	if len(firstSession.ks) == 0 ||
		len(secondSession.ks) > 0 ||
		len(thirdSession.ks) == 0 {
		t.Fatal("should have received blocks but didn't")
	}

	sm.ReceiveFrom(p, []cid.Cid{}, []cid.Cid{block.Cid()}, []cid.Cid{})
	if len(firstSession.wantBlocks) == 0 ||
		len(secondSession.wantBlocks) > 0 ||
		len(thirdSession.wantBlocks) == 0 {
		t.Fatal("should have received want-blocks but didn't")
	}

	sm.ReceiveFrom(p, []cid.Cid{}, []cid.Cid{}, []cid.Cid{block.Cid()})
	if len(firstSession.wantHaves) == 0 ||
		len(secondSession.wantHaves) > 0 ||
		len(thirdSession.wantHaves) == 0 {
		t.Fatal("should have received want-haves but didn't")
	}
}

func TestReceiveBlocksWhenManagerContextCancelled(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	notif := notifications.New()
	defer notif.Shutdown()
	sim := bssim.New()
	bpm := bsbpm.New()
	pm := &fakePeerManager{}
	sm := New(ctx, sessionFactory, sim, peerManagerFactory, bpm, pm, notif, "")

	p := peer.ID(123)
	block := blocks.NewBlock([]byte("block"))

	firstSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	secondSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	thirdSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)

	sim.RecordSessionInterest(firstSession.ID(), []cid.Cid{block.Cid()})
	sim.RecordSessionInterest(secondSession.ID(), []cid.Cid{block.Cid()})
	sim.RecordSessionInterest(thirdSession.ID(), []cid.Cid{block.Cid()})

	cancel()

	// wait for sessions to get removed
	time.Sleep(10 * time.Millisecond)

	sm.ReceiveFrom(p, []cid.Cid{block.Cid()}, []cid.Cid{}, []cid.Cid{})
	if len(firstSession.ks) > 0 ||
		len(secondSession.ks) > 0 ||
		len(thirdSession.ks) > 0 {
		t.Fatal("received blocks for sessions after manager is shutdown")
	}
}

func TestReceiveBlocksWhenSessionContextCancelled(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	notif := notifications.New()
	defer notif.Shutdown()
	sim := bssim.New()
	bpm := bsbpm.New()
	pm := &fakePeerManager{}
	sm := New(ctx, sessionFactory, sim, peerManagerFactory, bpm, pm, notif, "")

	p := peer.ID(123)
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

	sm.ReceiveFrom(p, []cid.Cid{block.Cid()}, []cid.Cid{}, []cid.Cid{})
	if len(firstSession.ks) == 0 ||
		len(secondSession.ks) > 0 ||
		len(thirdSession.ks) == 0 {
		t.Fatal("received blocks for sessions that are canceled")
	}
}
