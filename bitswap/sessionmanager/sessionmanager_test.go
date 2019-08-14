package sessionmanager

import (
	"context"
	"testing"
	"time"

	delay "github.com/ipfs/go-ipfs-delay"

	bssession "github.com/ipfs/go-bitswap/session"
	bssd "github.com/ipfs/go-bitswap/sessiondata"
	"github.com/ipfs/go-bitswap/testutil"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type fakeSession struct {
	interested []cid.Cid
	blks       []blocks.Block
	id         uint64
	pm         *fakePeerManager
	srs        *fakeRequestSplitter
}

func (*fakeSession) GetBlock(context.Context, cid.Cid) (blocks.Block, error) {
	return nil, nil
}
func (*fakeSession) GetBlocks(context.Context, []cid.Cid) (<-chan blocks.Block, error) {
	return nil, nil
}
func (fs *fakeSession) InterestedIn(c cid.Cid) bool {
	for _, ic := range fs.interested {
		if c == ic {
			return true
		}
	}
	return false
}
func (fs *fakeSession) ReceiveBlocksFrom(p peer.ID, blks []blocks.Block) {
	fs.blks = append(fs.blks, blks...)
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

var nextInterestedIn []cid.Cid

func sessionFactory(ctx context.Context,
	id uint64,
	pm bssession.PeerManager,
	srs bssession.RequestSplitter,
	provSearchDelay time.Duration,
	rebroadcastDelay delay.D) Session {
	return &fakeSession{
		interested: nextInterestedIn,
		id:         id,
		pm:         pm.(*fakePeerManager),
		srs:        srs.(*fakeRequestSplitter),
	}
}

func peerManagerFactory(ctx context.Context, id uint64) bssession.PeerManager {
	return &fakePeerManager{id}
}

func requestSplitterFactory(ctx context.Context) bssession.RequestSplitter {
	return &fakeRequestSplitter{}
}

func cmpSessionCids(s *fakeSession, cids []cid.Cid) bool {
	return cmpBlockCids(s.blks, cids)
}

func cmpBlockCids(blks []blocks.Block, cids []cid.Cid) bool {
	if len(blks) != len(cids) {
		return false
	}
	for _, b := range blks {
		has := false
		for _, c := range cids {
			if c == b.Cid() {
				has = true
			}
		}
		if !has {
			return false
		}
	}
	return true
}

func TestAddingSessions(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sm := New(ctx, sessionFactory, peerManagerFactory, requestSplitterFactory)

	p := peer.ID(123)
	block := blocks.NewBlock([]byte("block"))
	// we'll be interested in all blocks for this test
	nextInterestedIn = []cid.Cid{block.Cid()}

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
	sm.ReceiveBlocksFrom(p, []blocks.Block{block})
	if len(firstSession.blks) == 0 ||
		len(secondSession.blks) == 0 ||
		len(thirdSession.blks) == 0 {
		t.Fatal("should have received blocks but didn't")
	}
}

func TestReceivingBlocksWhenNotInterested(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sm := New(ctx, sessionFactory, peerManagerFactory, requestSplitterFactory)

	p := peer.ID(123)
	blks := testutil.GenerateBlocksOfSize(3, 1024)
	var cids []cid.Cid
	for _, b := range blks {
		cids = append(cids, b.Cid())
	}

	nextInterestedIn = []cid.Cid{cids[0], cids[1]}
	firstSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	nextInterestedIn = []cid.Cid{cids[0]}
	secondSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	nextInterestedIn = []cid.Cid{}
	thirdSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)

	sm.ReceiveBlocksFrom(p, []blocks.Block{blks[0], blks[1]})

	if !cmpSessionCids(firstSession, []cid.Cid{cids[0], cids[1]}) ||
		!cmpSessionCids(secondSession, []cid.Cid{cids[0]}) ||
		!cmpSessionCids(thirdSession, []cid.Cid{}) {
		t.Fatal("did not receive correct blocks for sessions")
	}
}

func TestRemovingPeersWhenManagerContextCancelled(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	sm := New(ctx, sessionFactory, peerManagerFactory, requestSplitterFactory)

	p := peer.ID(123)
	block := blocks.NewBlock([]byte("block"))
	// we'll be interested in all blocks for this test
	nextInterestedIn = []cid.Cid{block.Cid()}
	firstSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	secondSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	thirdSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)

	cancel()
	// wait for sessions to get removed
	time.Sleep(10 * time.Millisecond)
	sm.ReceiveBlocksFrom(p, []blocks.Block{block})
	if len(firstSession.blks) > 0 ||
		len(secondSession.blks) > 0 ||
		len(thirdSession.blks) > 0 {
		t.Fatal("received blocks for sessions after manager is shutdown")
	}
}

func TestRemovingPeersWhenSessionContextCancelled(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sm := New(ctx, sessionFactory, peerManagerFactory, requestSplitterFactory)

	p := peer.ID(123)
	block := blocks.NewBlock([]byte("block"))
	// we'll be interested in all blocks for this test
	nextInterestedIn = []cid.Cid{block.Cid()}
	firstSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	sessionCtx, sessionCancel := context.WithCancel(ctx)
	secondSession := sm.NewSession(sessionCtx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)
	thirdSession := sm.NewSession(ctx, time.Second, delay.Fixed(time.Minute)).(*fakeSession)

	sessionCancel()
	// wait for sessions to get removed
	time.Sleep(10 * time.Millisecond)
	sm.ReceiveBlocksFrom(p, []blocks.Block{block})
	if len(firstSession.blks) == 0 ||
		len(secondSession.blks) > 0 ||
		len(thirdSession.blks) == 0 {
		t.Fatal("received blocks for sessions that are canceled")
	}
}
