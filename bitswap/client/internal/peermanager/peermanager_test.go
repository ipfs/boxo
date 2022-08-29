package peermanager

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/internal/testutil"
	cid "github.com/ipfs/go-cid"

	"github.com/libp2p/go-libp2p/core/peer"
)

type msg struct {
	p          peer.ID
	wantBlocks []cid.Cid
	wantHaves  []cid.Cid
	cancels    []cid.Cid
}

type mockPeerQueue struct {
	p    peer.ID
	msgs chan msg
}

func (fp *mockPeerQueue) Startup()  {}
func (fp *mockPeerQueue) Shutdown() {}

func (fp *mockPeerQueue) AddBroadcastWantHaves(whs []cid.Cid) {
	fp.msgs <- msg{fp.p, nil, whs, nil}
}
func (fp *mockPeerQueue) AddWants(wbs []cid.Cid, whs []cid.Cid) {
	fp.msgs <- msg{fp.p, wbs, whs, nil}
}
func (fp *mockPeerQueue) AddCancels(cs []cid.Cid) {
	fp.msgs <- msg{fp.p, nil, nil, cs}
}
func (fp *mockPeerQueue) ResponseReceived(ks []cid.Cid) {
}

type peerWants struct {
	wantHaves  []cid.Cid
	wantBlocks []cid.Cid
	cancels    []cid.Cid
}

func collectMessages(ch chan msg, timeout time.Duration) map[peer.ID]peerWants {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	collected := make(map[peer.ID]peerWants)
	for {
		select {
		case m := <-ch:
			pw, ok := collected[m.p]
			if !ok {
				pw = peerWants{}
			}
			pw.wantHaves = append(pw.wantHaves, m.wantHaves...)
			pw.wantBlocks = append(pw.wantBlocks, m.wantBlocks...)
			pw.cancels = append(pw.cancels, m.cancels...)
			collected[m.p] = pw
		case <-ctx.Done():
			return collected
		}
	}
}

func makePeerQueueFactory(msgs chan msg) PeerQueueFactory {
	return func(ctx context.Context, p peer.ID) PeerQueue {
		return &mockPeerQueue{
			p:    p,
			msgs: msgs,
		}
	}
}

func TestAddingAndRemovingPeers(t *testing.T) {
	ctx := context.Background()
	msgs := make(chan msg, 16)
	peerQueueFactory := makePeerQueueFactory(msgs)

	tp := testutil.GeneratePeers(6)
	self, peer1, peer2, peer3, peer4, peer5 := tp[0], tp[1], tp[2], tp[3], tp[4], tp[5]
	peerManager := New(ctx, peerQueueFactory, self)

	peerManager.Connected(peer1)
	peerManager.Connected(peer2)
	peerManager.Connected(peer3)

	connectedPeers := peerManager.ConnectedPeers()

	if !testutil.ContainsPeer(connectedPeers, peer1) ||
		!testutil.ContainsPeer(connectedPeers, peer2) ||
		!testutil.ContainsPeer(connectedPeers, peer3) {
		t.Fatal("Peers not connected that should be connected")
	}

	if testutil.ContainsPeer(connectedPeers, peer4) ||
		testutil.ContainsPeer(connectedPeers, peer5) {
		t.Fatal("Peers connected that shouldn't be connected")
	}

	// disconnect a peer
	peerManager.Disconnected(peer1)
	connectedPeers = peerManager.ConnectedPeers()

	if testutil.ContainsPeer(connectedPeers, peer1) {
		t.Fatal("Peer should have been disconnected but was not")
	}

	// reconnect peer
	peerManager.Connected(peer1)
	connectedPeers = peerManager.ConnectedPeers()

	if !testutil.ContainsPeer(connectedPeers, peer1) {
		t.Fatal("Peer should have been connected but was not")
	}
}

func TestBroadcastOnConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msgs := make(chan msg, 16)
	peerQueueFactory := makePeerQueueFactory(msgs)
	tp := testutil.GeneratePeers(2)
	self, peer1 := tp[0], tp[1]
	peerManager := New(ctx, peerQueueFactory, self)

	cids := testutil.GenerateCids(2)
	peerManager.BroadcastWantHaves(ctx, cids)

	// Connect with two broadcast wants for first peer
	peerManager.Connected(peer1)
	collected := collectMessages(msgs, 2*time.Millisecond)

	if len(collected[peer1].wantHaves) != 2 {
		t.Fatal("Expected want-haves to be sent to newly connected peer")
	}
}

func TestBroadcastWantHaves(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msgs := make(chan msg, 16)
	peerQueueFactory := makePeerQueueFactory(msgs)
	tp := testutil.GeneratePeers(3)
	self, peer1, peer2 := tp[0], tp[1], tp[2]
	peerManager := New(ctx, peerQueueFactory, self)

	cids := testutil.GenerateCids(3)

	// Broadcast the first two.
	peerManager.BroadcastWantHaves(ctx, cids[:2])

	// First peer should get them.
	peerManager.Connected(peer1)
	collected := collectMessages(msgs, 2*time.Millisecond)

	if len(collected[peer1].wantHaves) != 2 {
		t.Fatal("Expected want-haves to be sent to newly connected peer")
	}

	// Connect to second peer
	peerManager.Connected(peer2)

	// Send a broadcast to all peers, including cid that was already sent to
	// first peer
	peerManager.BroadcastWantHaves(ctx, []cid.Cid{cids[0], cids[2]})
	collected = collectMessages(msgs, 2*time.Millisecond)

	// One of the want-haves was already sent to peer1
	if len(collected[peer1].wantHaves) != 1 {
		t.Fatalf("Expected 1 want-haves to be sent to first peer, got %d",
			len(collected[peer1].wantHaves))
	}
	if len(collected[peer2].wantHaves) != 3 {
		t.Fatalf("Expected 3 want-haves to be sent to second peer, got %d",
			len(collected[peer2].wantHaves))
	}
}

func TestSendWants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msgs := make(chan msg, 16)
	peerQueueFactory := makePeerQueueFactory(msgs)
	tp := testutil.GeneratePeers(2)
	self, peer1 := tp[0], tp[1]
	peerManager := New(ctx, peerQueueFactory, self)
	cids := testutil.GenerateCids(4)

	peerManager.Connected(peer1)
	peerManager.SendWants(ctx, peer1, []cid.Cid{cids[0]}, []cid.Cid{cids[2]})
	collected := collectMessages(msgs, 2*time.Millisecond)

	if len(collected[peer1].wantHaves) != 1 {
		t.Fatal("Expected want-have to be sent to peer")
	}
	if len(collected[peer1].wantBlocks) != 1 {
		t.Fatal("Expected want-block to be sent to peer")
	}

	peerManager.SendWants(ctx, peer1, []cid.Cid{cids[0], cids[1]}, []cid.Cid{cids[2], cids[3]})
	collected = collectMessages(msgs, 2*time.Millisecond)

	// First want-have and want-block should be filtered (because they were
	// already sent)
	if len(collected[peer1].wantHaves) != 1 {
		t.Fatal("Expected want-have to be sent to peer")
	}
	if len(collected[peer1].wantBlocks) != 1 {
		t.Fatal("Expected want-block to be sent to peer")
	}
}

func TestSendCancels(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msgs := make(chan msg, 16)
	peerQueueFactory := makePeerQueueFactory(msgs)
	tp := testutil.GeneratePeers(3)
	self, peer1, peer2 := tp[0], tp[1], tp[2]
	peerManager := New(ctx, peerQueueFactory, self)
	cids := testutil.GenerateCids(4)

	// Connect to peer1 and peer2
	peerManager.Connected(peer1)
	peerManager.Connected(peer2)

	// Send 2 want-blocks and 1 want-have to peer1
	peerManager.SendWants(ctx, peer1, []cid.Cid{cids[0], cids[1]}, []cid.Cid{cids[2]})

	// Clear messages
	collectMessages(msgs, 2*time.Millisecond)

	// Send cancels for 1 want-block and 1 want-have
	peerManager.SendCancels(ctx, []cid.Cid{cids[0], cids[2]})
	collected := collectMessages(msgs, 2*time.Millisecond)

	if _, ok := collected[peer2]; ok {
		t.Fatal("Expected no cancels to be sent to peer that was not sent messages")
	}
	if len(collected[peer1].cancels) != 2 {
		t.Fatal("Expected cancel to be sent for want-block and want-have sent to peer")
	}

	// Send cancels for all cids
	peerManager.SendCancels(ctx, cids)
	collected = collectMessages(msgs, 2*time.Millisecond)

	if _, ok := collected[peer2]; ok {
		t.Fatal("Expected no cancels to be sent to peer that was not sent messages")
	}
	if len(collected[peer1].cancels) != 1 {
		t.Fatal("Expected cancel to be sent for remaining want-block")
	}
}

func (s *sess) ID() uint64 {
	return s.id
}
func (s *sess) SignalAvailability(p peer.ID, isAvailable bool) {
	s.available[p] = isAvailable
}

type sess struct {
	id        uint64
	available map[peer.ID]bool
}

func newSess(id uint64) *sess {
	return &sess{id, make(map[peer.ID]bool)}
}

func TestSessionRegistration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msgs := make(chan msg, 16)
	peerQueueFactory := makePeerQueueFactory(msgs)

	tp := testutil.GeneratePeers(3)
	self, p1, p2 := tp[0], tp[1], tp[2]
	peerManager := New(ctx, peerQueueFactory, self)

	id := uint64(1)
	s := newSess(id)
	peerManager.RegisterSession(p1, s)
	if s.available[p1] {
		t.Fatal("Expected peer not be available till connected")
	}
	peerManager.RegisterSession(p2, s)
	if s.available[p2] {
		t.Fatal("Expected peer not be available till connected")
	}

	peerManager.Connected(p1)
	if !s.available[p1] {
		t.Fatal("Expected signal callback")
	}
	peerManager.Connected(p2)
	if !s.available[p2] {
		t.Fatal("Expected signal callback")
	}

	peerManager.Disconnected(p1)
	if s.available[p1] {
		t.Fatal("Expected signal callback")
	}
	if !s.available[p2] {
		t.Fatal("Expected signal callback only for disconnected peer")
	}

	peerManager.UnregisterSession(id)

	peerManager.Connected(p1)
	if s.available[p1] {
		t.Fatal("Expected no signal callback (session unregistered)")
	}
}

type benchPeerQueue struct {
}

func (*benchPeerQueue) Startup()  {}
func (*benchPeerQueue) Shutdown() {}

func (*benchPeerQueue) AddBroadcastWantHaves(whs []cid.Cid)   {}
func (*benchPeerQueue) AddWants(wbs []cid.Cid, whs []cid.Cid) {}
func (*benchPeerQueue) AddCancels(cs []cid.Cid)               {}
func (*benchPeerQueue) ResponseReceived(ks []cid.Cid)         {}

// Simplistic benchmark to allow us to stress test
func BenchmarkPeerManager(b *testing.B) {
	b.StopTimer()

	ctx := context.Background()

	peerQueueFactory := func(ctx context.Context, p peer.ID) PeerQueue {
		return &benchPeerQueue{}
	}

	self := testutil.GeneratePeers(1)[0]
	peers := testutil.GeneratePeers(500)
	peerManager := New(ctx, peerQueueFactory, self)

	// Create a bunch of connections
	connected := 0
	for i := 0; i < len(peers); i++ {
		peerManager.Connected(peers[i])
		connected++
	}

	var wanted []cid.Cid

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		// Pick a random peer
		i := rand.Intn(connected)

		// Alternately add either a few wants or many broadcast wants
		r := rand.Intn(8)
		if r == 0 {
			wants := testutil.GenerateCids(10)
			peerManager.SendWants(ctx, peers[i], wants[:2], wants[2:])
			wanted = append(wanted, wants...)
		} else if r == 1 {
			wants := testutil.GenerateCids(30)
			peerManager.BroadcastWantHaves(ctx, wants)
			wanted = append(wanted, wants...)
		} else {
			limit := len(wanted) / 10
			cancel := wanted[:limit]
			wanted = wanted[limit:]
			peerManager.SendCancels(ctx, cancel)
		}
	}
}
