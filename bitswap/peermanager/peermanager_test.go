package peermanager

import (
	"context"
	"testing"

	bsmsg "github.com/ipfs/go-bitswap/message"
	wantlist "github.com/ipfs/go-bitswap/wantlist"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs-blocksutil"
	"github.com/libp2p/go-libp2p-peer"
)

var blockGenerator = blocksutil.NewBlockGenerator()

func generateCids(n int) []cid.Cid {
	cids := make([]cid.Cid, 0, n)
	for i := 0; i < n; i++ {
		c := blockGenerator.Next().Cid()
		cids = append(cids, c)
	}
	return cids
}

var peerSeq int

func generatePeers(n int) []peer.ID {
	peerIds := make([]peer.ID, 0, n)
	for i := 0; i < n; i++ {
		peerSeq++
		p := peer.ID(peerSeq)
		peerIds = append(peerIds, p)
	}
	return peerIds
}

var nextSession uint64

func generateSessionID() uint64 {
	nextSession++
	return uint64(nextSession)
}

type messageSent struct {
	p       peer.ID
	entries []*bsmsg.Entry
	ses     uint64
}

type fakePeer struct {
	refcnt       int
	p            peer.ID
	messagesSent chan messageSent
}

func containsPeer(peers []peer.ID, p peer.ID) bool {
	for _, n := range peers {
		if p == n {
			return true
		}
	}
	return false
}

func (fp *fakePeer) Startup(ctx context.Context, initialEntries []*wantlist.Entry) {}
func (fp *fakePeer) Shutdown()                                                     {}
func (fp *fakePeer) RefIncrement()                                                 { fp.refcnt++ }
func (fp *fakePeer) RefDecrement() bool {
	fp.refcnt--
	return fp.refcnt > 0
}
func (fp *fakePeer) AddMessage(entries []*bsmsg.Entry, ses uint64) {
	fp.messagesSent <- messageSent{fp.p, entries, ses}
}

func makePeerQueueFactory(messagesSent chan messageSent) PeerQueueFactory {
	return func(p peer.ID) PeerQueue {
		return &fakePeer{
			p:            p,
			refcnt:       1,
			messagesSent: messagesSent,
		}
	}
}

func TestAddingAndRemovingPeers(t *testing.T) {
	ctx := context.Background()
	peerQueueFactory := makePeerQueueFactory(nil)

	tp := generatePeers(5)
	peer1, peer2, peer3, peer4, peer5 := tp[0], tp[1], tp[2], tp[3], tp[4]
	peerManager := New(ctx, peerQueueFactory)
	peerManager.Startup()

	peerManager.Connected(peer1, nil)
	peerManager.Connected(peer2, nil)
	peerManager.Connected(peer3, nil)

	connectedPeers := peerManager.ConnectedPeers()

	if !containsPeer(connectedPeers, peer1) ||
		!containsPeer(connectedPeers, peer2) ||
		!containsPeer(connectedPeers, peer3) {
		t.Fatal("Peers not connected that should be connected")
	}

	if containsPeer(connectedPeers, peer4) ||
		containsPeer(connectedPeers, peer5) {
		t.Fatal("Peers connected that shouldn't be connected")
	}

	// removing a peer with only one reference
	peerManager.Disconnected(peer1)
	connectedPeers = peerManager.ConnectedPeers()

	if containsPeer(connectedPeers, peer1) {
		t.Fatal("Peer should have been disconnected but was not")
	}

	// connecting a peer twice, then disconnecting once, should stay in queue
	peerManager.Connected(peer2, nil)
	peerManager.Disconnected(peer2)
	connectedPeers = peerManager.ConnectedPeers()

	if !containsPeer(connectedPeers, peer2) {
		t.Fatal("Peer was disconnected but should not have been")
	}
}
