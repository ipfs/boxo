package sessionmanager

import (
	"context"
	"testing"
	"time"

	delay "github.com/ipfs/go-ipfs-delay"

	notifications "github.com/ipfs/go-bitswap/notifications"
	bssession "github.com/ipfs/go-bitswap/session"
	bssd "github.com/ipfs/go-bitswap/sessiondata"
	"github.com/ipfs/go-bitswap/testutil"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type fakeSession struct {
	wanted []cid.Cid
	ks     []cid.Cid
	id     uint64
	pm     *fakePeerManager
	srs    *fakeRequestSplitter
	notif  notifications.PubSub
}

func (*fakeSession) GetBlock(context.Context, cid.Cid) (blocks.Block, error) {
	return nil, nil
}
func (*fakeSession) GetBlocks(context.Context, []cid.Cid) (<-chan blocks.Block, error) {
	return nil, nil
}
func (fs *fakeSession) IsWanted(c cid.Cid) bool {
	for _, ic := range fs.wanted {
		if c == ic {
			return true
		}
	}
	return false
}
func (fs *fakeSession) ReceiveFrom(p peer.ID, ks []cid.Cid) {
	fs.ks = append(fs.ks, ks...)
}

type fakePeerManager struct {
	id uint64
}

func (*fakePeerManager) FindMorePeers(context.Context, cid.Cid)  {}
func (*fakePeerManager) GetOptimizedPeers() []bssd.OptimizedPeer { return nil }
func (*fakePeerManager) RecordPeerRequests([]peer.ID, []cid.Cid) {}
func (*fakePeerManager) RecordPeerResponse(peer.ID, []cid.Cid)   {}
func (*fakePeerManager) RecordCancels(c []cid.Cid)               {}

type fakeRequestSplitter struct {
}

func (frs *fakeRequestSplitter) SplitRequest(optimizedPeers []bssd.OptimizedPeer, keys []cid.Cid) []bssd.PartialRequest {
	return nil
}
func (frs *fakeRequestSplitter) RecordDuplicateBlock() {}
func (frs *fakeRequestSplitter) RecordUniqueBlock()    {}

var nextWanted []cid.Cid

func sessionFactory(ctx context.Context,
	id uint64,
	pm bssession.PeerManager,
	srs bssession.RequestSplitter,
	notif notifications.PubSub,
	provSearchDelay time.Duration,
	rebroadcastDelay delay.D) Session {
	return &fakeSession{
		wanted: nextWanted,
		id:     id,
		pm:     pm.(*fakePeerManager),
		srs:    srs.(*fakeRequestSplitter),
		notif:  notif,
	}
}

func peerManagerFactory(ctx context.Context, id uint64) bssession.PeerManager {
	return &fakePeerManager{id}
}

func requestSplitterFactory(ctx context.Context) bssession.RequestSplitter {
	return &fakeRequestSplitter{}
}

func TestAddingSessions(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	notif := notifications.New()
	defer notif.Shutdown()
	sm := New(ctx, sessionFactory, peerManagerFactory, requestSplitterFactory, notif)

	p := peer.ID(123)
	block := blocks.NewBlock([]byte("block"))
	// we'll be interested in all blocks for this test
	nextWanted = []cid.Cid{block.Cid()}

	currentID := sm.GetNextSessionID()
	firstSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	if firstSession.id != firstSession.pm.id ||
		firstSession.id != currentID+1 {
		t.Fatal("session does not have correct id set")
	}
	secondSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	if secondSession.id != secondSession.pm.id ||
		secondSession.id != firstSession.id+1 {
		t.Fatal("session does not have correct id set")
	}
	sm.GetNextSessionID()
	thirdSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	if thirdSession.id != thirdSession.pm.id ||
		thirdSession.id != secondSession.id+2 {
		t.Fatal("session does not have correct id set")
	}
	sm.ReceiveFrom(p, []cid.Cid{block.Cid()})
	if len(firstSession.ks) == 0 ||
		len(secondSession.ks) == 0 ||
		len(thirdSession.ks) == 0 {
		t.Fatal("should have received blocks but didn't")
	}
}

func TestIsWanted(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	notif := notifications.New()
	defer notif.Shutdown()
	sm := New(ctx, sessionFactory, peerManagerFactory, requestSplitterFactory, notif)

	blks := testutil.GenerateBlocksOfSize(4, 1024)
	var cids []cid.Cid
	for _, b := range blks {
		cids = append(cids, b.Cid())
	}

	nextWanted = []cid.Cid{cids[0], cids[1]}
	_ = sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	nextWanted = []cid.Cid{cids[0], cids[2]}
	_ = sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)

	if !sm.IsWanted(cids[0]) ||
		!sm.IsWanted(cids[1]) ||
		!sm.IsWanted(cids[2]) {
		t.Fatal("expected unwanted but session manager did want cid")
	}
	if sm.IsWanted(cids[3]) {
		t.Fatal("expected wanted but session manager did not want cid")
	}
}

func TestRemovingPeersWhenManagerContextCancelled(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	notif := notifications.New()
	defer notif.Shutdown()
	sm := New(ctx, sessionFactory, peerManagerFactory, requestSplitterFactory, notif)

	p := peer.ID(123)
	block := blocks.NewBlock([]byte("block"))
	// we'll be interested in all blocks for this test
	nextWanted = []cid.Cid{block.Cid()}
	firstSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	secondSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	thirdSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)

	cancel()
	// wait for sessions to get removed
	time.Sleep(10 * time.Millisecond)
	sm.ReceiveFrom(p, []cid.Cid{block.Cid()})
	if len(firstSession.ks) > 0 ||
		len(secondSession.ks) > 0 ||
		len(thirdSession.ks) > 0 {
		t.Fatal("received blocks for sessions after manager is shutdown")
	}
}

func TestRemovingPeersWhenSessionContextCancelled(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	notif := notifications.New()
	defer notif.Shutdown()
	sm := New(ctx, sessionFactory, peerManagerFactory, requestSplitterFactory, notif)

	p := peer.ID(123)
	block := blocks.NewBlock([]byte("block"))
	// we'll be interested in all blocks for this test
	nextWanted = []cid.Cid{block.Cid()}
	firstSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	sessionCtx, sessionCancel := context.WithCancel(ctx)
	secondSession := sm.NewSession(sessionCtx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	thirdSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)

	sessionCancel()
	// wait for sessions to get removed
	time.Sleep(10 * time.Millisecond)
	sm.ReceiveFrom(p, []cid.Cid{block.Cid()})
	if len(firstSession.ks) == 0 ||
		len(secondSession.ks) > 0 ||
		len(thirdSession.ks) == 0 {
		t.Fatal("received blocks for sessions that are canceled")
	}
}
