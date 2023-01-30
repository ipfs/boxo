package session

import (
	"testing"

	"github.com/ipfs/go-libipfs/bitswap/internal/testutil"
	"github.com/ipfs/go-libipfs/internal/test"
)

func TestEmptyWantInfo(t *testing.T) {
	test.Flaky(t)

	wp := newWantInfo(newPeerResponseTracker())

	if wp.bestPeer != "" {
		t.Fatal("expected no best peer")
	}
}

func TestSetPeerBlockPresence(t *testing.T) {
	test.Flaky(t)

	peers := testutil.GeneratePeers(2)
	wp := newWantInfo(newPeerResponseTracker())

	wp.setPeerBlockPresence(peers[0], BPUnknown)
	if wp.bestPeer != peers[0] {
		t.Fatal("wrong best peer")
	}

	wp.setPeerBlockPresence(peers[1], BPHave)
	if wp.bestPeer != peers[1] {
		t.Fatal("wrong best peer")
	}

	wp.setPeerBlockPresence(peers[0], BPDontHave)
	if wp.bestPeer != peers[1] {
		t.Fatal("wrong best peer")
	}
}

func TestSetPeerBlockPresenceBestLower(t *testing.T) {
	test.Flaky(t)

	peers := testutil.GeneratePeers(2)
	wp := newWantInfo(newPeerResponseTracker())

	wp.setPeerBlockPresence(peers[0], BPHave)
	if wp.bestPeer != peers[0] {
		t.Fatal("wrong best peer")
	}

	wp.setPeerBlockPresence(peers[1], BPUnknown)
	if wp.bestPeer != peers[0] {
		t.Fatal("wrong best peer")
	}

	wp.setPeerBlockPresence(peers[0], BPDontHave)
	if wp.bestPeer != peers[1] {
		t.Fatal("wrong best peer")
	}
}

func TestRemoveThenSetDontHave(t *testing.T) {
	test.Flaky(t)

	peers := testutil.GeneratePeers(2)
	wp := newWantInfo(newPeerResponseTracker())

	wp.setPeerBlockPresence(peers[0], BPUnknown)
	if wp.bestPeer != peers[0] {
		t.Fatal("wrong best peer")
	}

	wp.removePeer(peers[0])
	if wp.bestPeer != "" {
		t.Fatal("wrong best peer")
	}

	wp.setPeerBlockPresence(peers[1], BPUnknown)
	if wp.bestPeer != peers[1] {
		t.Fatal("wrong best peer")
	}

	wp.setPeerBlockPresence(peers[0], BPDontHave)
	if wp.bestPeer != peers[1] {
		t.Fatal("wrong best peer")
	}
}
