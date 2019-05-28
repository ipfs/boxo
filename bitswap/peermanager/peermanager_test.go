package peermanager

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ipfs/go-bitswap/testutil"

	bsmsg "github.com/ipfs/go-bitswap/message"
	wantlist "github.com/ipfs/go-bitswap/wantlist"
	"github.com/libp2p/go-libp2p-core/peer"
)

type messageSent struct {
	p       peer.ID
	entries []bsmsg.Entry
	ses     uint64
}

type fakePeer struct {
	p            peer.ID
	messagesSent chan messageSent
}

func (fp *fakePeer) Startup()  {}
func (fp *fakePeer) Shutdown() {}

func (fp *fakePeer) AddMessage(entries []bsmsg.Entry, ses uint64) {
	fp.messagesSent <- messageSent{fp.p, entries, ses}
}
func (fp *fakePeer) AddWantlist(initialWants *wantlist.SessionTrackedWantlist) {}
func makePeerQueueFactory(messagesSent chan messageSent) PeerQueueFactory {
	return func(ctx context.Context, p peer.ID) PeerQueue {
		return &fakePeer{
			p:            p,
			messagesSent: messagesSent,
		}
	}
}

func collectAndCheckMessages(
	ctx context.Context,
	t *testing.T,
	messagesSent <-chan messageSent,
	entries []bsmsg.Entry,
	ses uint64,
	timeout time.Duration) []peer.ID {
	var peersReceived []peer.ID
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		select {
		case nextMessage := <-messagesSent:
			if nextMessage.ses != ses {
				t.Fatal("Message enqueued with wrong session")
			}
			if !reflect.DeepEqual(nextMessage.entries, entries) {
				t.Fatal("Message enqueued with wrong wants")
			}
			peersReceived = append(peersReceived, nextMessage.p)
		case <-timeoutCtx.Done():
			return peersReceived
		}
	}
}

func TestAddingAndRemovingPeers(t *testing.T) {
	ctx := context.Background()
	peerQueueFactory := makePeerQueueFactory(nil)

	tp := testutil.GeneratePeers(5)
	peer1, peer2, peer3, peer4, peer5 := tp[0], tp[1], tp[2], tp[3], tp[4]
	peerManager := New(ctx, peerQueueFactory)

	peerManager.Connected(peer1, nil)
	peerManager.Connected(peer2, nil)
	peerManager.Connected(peer3, nil)

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

	// removing a peer with only one reference
	peerManager.Disconnected(peer1)
	connectedPeers = peerManager.ConnectedPeers()

	if testutil.ContainsPeer(connectedPeers, peer1) {
		t.Fatal("Peer should have been disconnected but was not")
	}

	// connecting a peer twice, then disconnecting once, should stay in queue
	peerManager.Connected(peer2, nil)
	peerManager.Disconnected(peer2)
	connectedPeers = peerManager.ConnectedPeers()

	if !testutil.ContainsPeer(connectedPeers, peer2) {
		t.Fatal("Peer was disconnected but should not have been")
	}
}

func TestSendingMessagesToPeers(t *testing.T) {
	ctx := context.Background()
	messagesSent := make(chan messageSent, 16)
	peerQueueFactory := makePeerQueueFactory(messagesSent)

	tp := testutil.GeneratePeers(5)

	peer1, peer2, peer3, peer4, peer5 := tp[0], tp[1], tp[2], tp[3], tp[4]
	peerManager := New(ctx, peerQueueFactory)

	peerManager.Connected(peer1, nil)
	peerManager.Connected(peer2, nil)
	peerManager.Connected(peer3, nil)

	entries := testutil.GenerateMessageEntries(5, false)
	ses := testutil.GenerateSessionID()

	peerManager.SendMessage(entries, nil, ses)

	peersReceived := collectAndCheckMessages(
		ctx, t, messagesSent, entries, ses, 10*time.Millisecond)
	if len(peersReceived) != 3 {
		t.Fatal("Incorrect number of peers received messages")
	}

	if !testutil.ContainsPeer(peersReceived, peer1) ||
		!testutil.ContainsPeer(peersReceived, peer2) ||
		!testutil.ContainsPeer(peersReceived, peer3) {
		t.Fatal("Peers should have received message but did not")
	}

	if testutil.ContainsPeer(peersReceived, peer4) ||
		testutil.ContainsPeer(peersReceived, peer5) {
		t.Fatal("Peers received message but should not have")
	}

	var peersToSendTo []peer.ID
	peersToSendTo = append(peersToSendTo, peer1, peer3, peer4)
	peerManager.SendMessage(entries, peersToSendTo, ses)
	peersReceived = collectAndCheckMessages(
		ctx, t, messagesSent, entries, ses, 10*time.Millisecond)

	if len(peersReceived) != 3 {
		t.Fatal("Incorrect number of peers received messages")
	}

	if !testutil.ContainsPeer(peersReceived, peer1) ||
		!testutil.ContainsPeer(peersReceived, peer3) {
		t.Fatal("Peers should have received message but did not")
	}

	if testutil.ContainsPeer(peersReceived, peer2) ||
		testutil.ContainsPeer(peersReceived, peer5) {
		t.Fatal("Peers received message but should not have")
	}

	if !testutil.ContainsPeer(peersReceived, peer4) {
		t.Fatal("Peer should have autoconnected on message send")
	}
}
