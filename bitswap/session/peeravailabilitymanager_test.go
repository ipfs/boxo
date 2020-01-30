package session

import (
	"testing"

	"github.com/ipfs/go-bitswap/testutil"
)

func TestPeerAvailabilityManager(t *testing.T) {
	peers := testutil.GeneratePeers(2)
	pam := newPeerAvailabilityManager()

	isAvailable, ok := pam.isAvailable(peers[0])
	if isAvailable || ok {
		t.Fatal("expected not to have any availability yet")
	}

	if pam.haveAvailablePeers() {
		t.Fatal("expected not to have any availability yet")
	}

	pam.addPeer(peers[0])
	isAvailable, ok = pam.isAvailable(peers[0])
	if !ok {
		t.Fatal("expected to have a peer")
	}
	if isAvailable {
		t.Fatal("expected not to have any availability yet")
	}
	if pam.haveAvailablePeers() {
		t.Fatal("expected not to have any availability yet")
	}
	if len(pam.availablePeers()) != 0 {
		t.Fatal("expected not to have any availability yet")
	}
	if len(pam.allPeers()) != 1 {
		t.Fatal("expected one peer")
	}

	pam.setPeerAvailability(peers[0], true)
	isAvailable, ok = pam.isAvailable(peers[0])
	if !ok {
		t.Fatal("expected to have a peer")
	}
	if !isAvailable {
		t.Fatal("expected peer to be available")
	}
	if !pam.haveAvailablePeers() {
		t.Fatal("expected peer to be available")
	}
	if len(pam.availablePeers()) != 1 {
		t.Fatal("expected peer to be available")
	}
	if len(pam.allPeers()) != 1 {
		t.Fatal("expected one peer")
	}

	pam.addPeer(peers[1])
	if len(pam.availablePeers()) != 1 {
		t.Fatal("expected one peer to be available")
	}
	if len(pam.allPeers()) != 2 {
		t.Fatal("expected two peers")
	}

	pam.setPeerAvailability(peers[0], false)
	isAvailable, ok = pam.isAvailable(peers[0])
	if !ok {
		t.Fatal("expected to have a peer")
	}
	if isAvailable {
		t.Fatal("expected peer to not be available")
	}
}
