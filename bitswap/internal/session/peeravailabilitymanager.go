package session

import (
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// peerAvailabilityManager keeps track of which peers have available space
// to receive want requests
type peerAvailabilityManager struct {
	peerAvailable map[peer.ID]bool
}

func newPeerAvailabilityManager() *peerAvailabilityManager {
	return &peerAvailabilityManager{
		peerAvailable: make(map[peer.ID]bool),
	}
}

func (pam *peerAvailabilityManager) addPeer(p peer.ID) {
	pam.peerAvailable[p] = false
}

func (pam *peerAvailabilityManager) isAvailable(p peer.ID) (bool, bool) {
	is, ok := pam.peerAvailable[p]
	return is, ok
}

func (pam *peerAvailabilityManager) setPeerAvailability(p peer.ID, isAvailable bool) {
	pam.peerAvailable[p] = isAvailable
}

func (pam *peerAvailabilityManager) haveAvailablePeers() bool {
	for _, isAvailable := range pam.peerAvailable {
		if isAvailable {
			return true
		}
	}
	return false
}

func (pam *peerAvailabilityManager) availablePeers() []peer.ID {
	var available []peer.ID
	for p, isAvailable := range pam.peerAvailable {
		if isAvailable {
			available = append(available, p)
		}
	}
	return available
}

func (pam *peerAvailabilityManager) allPeers() []peer.ID {
	var available []peer.ID
	for p := range pam.peerAvailable {
		available = append(available, p)
	}
	return available
}
