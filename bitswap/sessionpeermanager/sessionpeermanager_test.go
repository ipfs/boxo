package sessionpeermanager

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/testutil"

	cid "github.com/ipfs/go-cid"
	ifconnmgr "github.com/libp2p/go-libp2p-interface-connmgr"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
)

type fakePeerNetwork struct {
	peers       []peer.ID
	connManager ifconnmgr.ConnManager
}

func (fpn *fakePeerNetwork) ConnectionManager() ifconnmgr.ConnManager {
	return fpn.connManager
}

func (fpn *fakePeerNetwork) FindProvidersAsync(ctx context.Context, c cid.Cid, num int) <-chan peer.ID {
	peerCh := make(chan peer.ID)
	go func() {
		defer close(peerCh)
		for _, p := range fpn.peers {
			select {
			case peerCh <- p:
			case <-ctx.Done():
				return
			}
		}
	}()
	return peerCh
}

type fakeConnManager struct {
	taggedPeers []peer.ID
}

func (fcm *fakeConnManager) TagPeer(p peer.ID, tag string, n int) {
	fcm.taggedPeers = append(fcm.taggedPeers, p)
}
func (fcm *fakeConnManager) UntagPeer(p peer.ID, tag string) {
	for i := 0; i < len(fcm.taggedPeers); i++ {
		if fcm.taggedPeers[i] == p {
			fcm.taggedPeers[i] = fcm.taggedPeers[len(fcm.taggedPeers)-1]
			fcm.taggedPeers = fcm.taggedPeers[:len(fcm.taggedPeers)-1]
			return
		}
	}
}
func (*fakeConnManager) GetTagInfo(p peer.ID) *ifconnmgr.TagInfo { return nil }
func (*fakeConnManager) TrimOpenConns(ctx context.Context)       {}
func (*fakeConnManager) Notifee() inet.Notifiee                  { return nil }

func TestFindingMorePeers(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	peers := testutil.GeneratePeers(5)
	fcm := &fakeConnManager{}
	fpn := &fakePeerNetwork{peers, fcm}
	c := testutil.GenerateCids(1)[0]
	id := testutil.GenerateSessionID()

	sessionPeerManager := New(ctx, id, fpn)

	findCtx, findCancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer findCancel()
	sessionPeerManager.FindMorePeers(ctx, c)
	<-findCtx.Done()
	sessionPeers := sessionPeerManager.GetOptimizedPeers()
	if len(sessionPeers) != len(peers) {
		t.Fatal("incorrect number of peers found")
	}
	for _, p := range sessionPeers {
		if !testutil.ContainsPeer(peers, p) {
			t.Fatal("incorrect peer found through finding providers")
		}
	}
	if len(fcm.taggedPeers) != len(peers) {
		t.Fatal("Peers were not tagged!")
	}
}

func TestRecordingReceivedBlocks(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	p := testutil.GeneratePeers(1)[0]
	fcm := &fakeConnManager{}
	fpn := &fakePeerNetwork{nil, fcm}
	c := testutil.GenerateCids(1)[0]
	id := testutil.GenerateSessionID()

	sessionPeerManager := New(ctx, id, fpn)
	sessionPeerManager.RecordPeerResponse(p, c)
	time.Sleep(10 * time.Millisecond)
	sessionPeers := sessionPeerManager.GetOptimizedPeers()
	if len(sessionPeers) != 1 {
		t.Fatal("did not add peer on receive")
	}
	if sessionPeers[0] != p {
		t.Fatal("incorrect peer added on receive")
	}
	if len(fcm.taggedPeers) != 1 {
		t.Fatal("Peers was not tagged!")
	}
}

func TestUntaggingPeers(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	peers := testutil.GeneratePeers(5)
	fcm := &fakeConnManager{}
	fpn := &fakePeerNetwork{peers, fcm}
	c := testutil.GenerateCids(1)[0]
	id := testutil.GenerateSessionID()

	sessionPeerManager := New(ctx, id, fpn)

	sessionPeerManager.FindMorePeers(ctx, c)
	time.Sleep(5 * time.Millisecond)
	if len(fcm.taggedPeers) != len(peers) {
		t.Fatal("Peers were not tagged!")
	}
	<-ctx.Done()
	if len(fcm.taggedPeers) != 0 {
		t.Fatal("Peers were not untagged!")
	}
}
