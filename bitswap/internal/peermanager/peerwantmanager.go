package peermanager

import (
	"bytes"
	"fmt"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Gauge can be used to keep track of a metric that increases and decreases
// incrementally. It is used by the peerWantManager to track the number of
// want-blocks that are active (ie sent but no response received)
type Gauge interface {
	Inc()
	Dec()
}

// peerWantManager keeps track of which want-haves and want-blocks have been
// sent to each peer, so that the PeerManager doesn't send duplicates.
type peerWantManager struct {
	// peerWants maps peers to outstanding wants.
	// A peer's wants is the _union_ of the broadcast wants and the wants in
	// this list.
	peerWants map[peer.ID]*peerWant

	// Reverse index of all wants in peerWants.
	wantPeers map[cid.Cid]map[peer.ID]struct{}

	// broadcastWants tracks all the current broadcast wants.
	broadcastWants *cid.Set

	// Keeps track of the number of active want-blocks
	wantBlockGauge Gauge
}

type peerWant struct {
	wantBlocks *cid.Set
	wantHaves  *cid.Set
}

// New creates a new peerWantManager with a Gauge that keeps track of the
// number of active want-blocks (ie sent but no response received)
func newPeerWantManager(wantBlockGauge Gauge) *peerWantManager {
	return &peerWantManager{
		broadcastWants: cid.NewSet(),
		peerWants:      make(map[peer.ID]*peerWant),
		wantPeers:      make(map[cid.Cid]map[peer.ID]struct{}),
		wantBlockGauge: wantBlockGauge,
	}
}

// addPeer adds a peer whose wants we need to keep track of. It returns the
// current list of broadcast wants that should be sent to the peer.
func (pwm *peerWantManager) addPeer(p peer.ID) []cid.Cid {
	if _, ok := pwm.peerWants[p]; !ok {
		pwm.peerWants[p] = &peerWant{
			wantBlocks: cid.NewSet(),
			wantHaves:  cid.NewSet(),
		}
		return pwm.broadcastWants.Keys()
	}
	return nil
}

// RemovePeer removes a peer and its associated wants from tracking
func (pwm *peerWantManager) removePeer(p peer.ID) {
	pws, ok := pwm.peerWants[p]
	if !ok {
		return
	}

	_ = pws.wantBlocks.ForEach(func(c cid.Cid) error {
		// Decrement the gauge by the number of pending want-blocks to the peer
		pwm.wantBlockGauge.Dec()
		// Clean up want-blocks from the reverse index
		pwm.reverseIndexRemove(c, p)
		return nil
	})

	// Clean up want-haves from the reverse index
	_ = pws.wantHaves.ForEach(func(c cid.Cid) error {
		pwm.reverseIndexRemove(c, p)
		return nil
	})

	delete(pwm.peerWants, p)
}

// PrepareBroadcastWantHaves filters the list of want-haves for each peer,
// returning a map of peers to the want-haves they have not yet been sent.
func (pwm *peerWantManager) prepareBroadcastWantHaves(wantHaves []cid.Cid) map[peer.ID][]cid.Cid {
	res := make(map[peer.ID][]cid.Cid, len(pwm.peerWants))
	for _, c := range wantHaves {
		if pwm.broadcastWants.Has(c) {
			// Already a broadcast want, skip it.
			continue
		}
		pwm.broadcastWants.Add(c)

		// Prepare broadcast.
		wantedBy := pwm.wantPeers[c]
		for p := range pwm.peerWants {
			// If we've already sent a want to this peer, skip them.
			//
			// This is faster than checking the actual wantlists due
			// to better locality.
			if _, ok := wantedBy[p]; ok {
				continue
			}

			cids, ok := res[p]
			if !ok {
				cids = make([]cid.Cid, 0, len(wantHaves))
			}
			res[p] = append(cids, c)
		}
	}

	return res
}

// PrepareSendWants filters the list of want-blocks and want-haves such that
// it only contains wants that have not already been sent to the peer.
func (pwm *peerWantManager) prepareSendWants(p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) ([]cid.Cid, []cid.Cid) {
	resWantBlks := make([]cid.Cid, 0)
	resWantHvs := make([]cid.Cid, 0)

	// Get the existing want-blocks and want-haves for the peer
	pws, ok := pwm.peerWants[p]

	if !ok {
		// In practice this should never happen:
		// - PeerManager calls addPeer() as soon as the peer connects
		// - PeerManager calls removePeer() as soon as the peer disconnects
		// - All calls to PeerWantManager are locked
		log.Errorf("prepareSendWants() called with peer %s but peer not found in peerWantManager", string(p))
		return resWantBlks, resWantHvs
	}

	// Iterate over the requested want-blocks
	for _, c := range wantBlocks {
		// If the want-block hasn't been sent to the peer
		if !pws.wantBlocks.Has(c) {
			// Record that the CID was sent as a want-block
			pws.wantBlocks.Add(c)

			// Update the reverse index
			pwm.reverseIndexAdd(c, p)

			// Add the CID to the results
			resWantBlks = append(resWantBlks, c)

			// Make sure the CID is no longer recorded as a want-have
			pws.wantHaves.Remove(c)

			// Increment the count of want-blocks
			pwm.wantBlockGauge.Inc()
		}
	}

	// Iterate over the requested want-haves
	for _, c := range wantHaves {
		// If we've already broadcasted this want, don't bother with a
		// want-have.
		if pwm.broadcastWants.Has(c) {
			continue
		}

		// If the CID has not been sent as a want-block or want-have
		if !pws.wantBlocks.Has(c) && !pws.wantHaves.Has(c) {
			// Record that the CID was sent as a want-have
			pws.wantHaves.Add(c)

			// Update the reverse index
			pwm.reverseIndexAdd(c, p)

			// Add the CID to the results
			resWantHvs = append(resWantHvs, c)
		}
	}

	return resWantBlks, resWantHvs
}

// PrepareSendCancels filters the list of cancels for each peer,
// returning a map of peers which only contains cancels for wants that have
// been sent to the peer.
func (pwm *peerWantManager) prepareSendCancels(cancelKs []cid.Cid) map[peer.ID][]cid.Cid {
	if len(cancelKs) == 0 {
		return nil
	}

	// Pre-allocate enough space for all peers that have the first CID.
	// Chances are these peers are related.
	expectedResSize := 0
	firstCancel := cancelKs[0]
	if pwm.broadcastWants.Has(firstCancel) {
		expectedResSize = len(pwm.peerWants)
	} else {
		expectedResSize = len(pwm.wantPeers[firstCancel])
	}
	res := make(map[peer.ID][]cid.Cid, expectedResSize)

	// Keep the broadcast keys separate. This lets us batch-process them at
	// the end.
	broadcastKs := make([]cid.Cid, 0, len(cancelKs))

	// Iterate over all requested cancels
	for _, c := range cancelKs {
		// Handle broadcast wants up-front.
		isBroadcast := pwm.broadcastWants.Has(c)
		if isBroadcast {
			broadcastKs = append(broadcastKs, c)
			pwm.broadcastWants.Remove(c)
		}

		// Even if this is a broadcast, we may have sent targeted wants.
		// Deal with them.
		for p := range pwm.wantPeers[c] {
			pws, ok := pwm.peerWants[p]
			if !ok {
				// Should never happen but check just in case
				log.Errorf("peerWantManager reverse index missing peer %s for key %s", p, c)
				continue
			}

			// Update the want gauge.
			if pws.wantBlocks.Has(c) {
				pwm.wantBlockGauge.Dec()
			}

			// Unconditionally remove from the want lists.
			pws.wantBlocks.Remove(c)
			pws.wantHaves.Remove(c)

			// If it's a broadcast want, we've already added it to
			// the broadcastKs list.
			if isBroadcast {
				continue
			}

			// Add the CID to the result for the peer.
			cids, ok := res[p]
			if !ok {
				// Pre-allocate enough for all keys.
				// Cancels are usually related.
				cids = make([]cid.Cid, 0, len(cancelKs))
			}
			res[p] = append(cids, c)
		}

		// Finally, batch-remove the reverse-index. There's no need to
		// clear this index peer-by-peer.
		delete(pwm.wantPeers, c)
	}

	// If we have any broadcasted CIDs, add them in.
	//
	// Doing this at the end can save us a bunch of work and allocations.
	if len(broadcastKs) > 0 {
		for p := range pwm.peerWants {
			if cids, ok := res[p]; ok {
				res[p] = append(cids, broadcastKs...)
			} else {
				res[p] = broadcastKs
			}
		}
	}

	return res
}

// Add the peer to the list of peers that have sent a want with the cid
func (pwm *peerWantManager) reverseIndexAdd(c cid.Cid, p peer.ID) {
	peers, ok := pwm.wantPeers[c]
	if !ok {
		peers = make(map[peer.ID]struct{}, 10)
		pwm.wantPeers[c] = peers
	}
	peers[p] = struct{}{}
}

// Remove the peer from the list of peers that have sent a want with the cid
func (pwm *peerWantManager) reverseIndexRemove(c cid.Cid, p peer.ID) {
	if peers, ok := pwm.wantPeers[c]; ok {
		delete(peers, p)
		if len(peers) == 0 {
			delete(pwm.wantPeers, c)
		}
	}
}

// GetWantBlocks returns the set of all want-blocks sent to all peers
func (pwm *peerWantManager) getWantBlocks() []cid.Cid {
	res := cid.NewSet()

	// Iterate over all known peers
	for _, pws := range pwm.peerWants {
		// Iterate over all want-blocks
		_ = pws.wantBlocks.ForEach(func(c cid.Cid) error {
			// Add the CID to the results
			res.Add(c)
			return nil
		})
	}

	return res.Keys()
}

// GetWantHaves returns the set of all want-haves sent to all peers
func (pwm *peerWantManager) getWantHaves() []cid.Cid {
	res := cid.NewSet()

	// Iterate over all peers with active wants.
	for _, pws := range pwm.peerWants {
		// Iterate over all want-haves
		_ = pws.wantHaves.ForEach(func(c cid.Cid) error {
			// Add the CID to the results
			res.Add(c)
			return nil
		})
	}
	_ = pwm.broadcastWants.ForEach(func(c cid.Cid) error {
		res.Add(c)
		return nil
	})

	return res.Keys()
}

// GetWants returns the set of all wants (both want-blocks and want-haves).
func (pwm *peerWantManager) getWants() []cid.Cid {
	res := pwm.broadcastWants.Keys()

	// Iterate over all targeted wants, removing ones that are also in the
	// broadcast list.
	for c := range pwm.wantPeers {
		if pwm.broadcastWants.Has(c) {
			continue
		}
		res = append(res, c)
	}

	return res
}

func (pwm *peerWantManager) String() string {
	var b bytes.Buffer
	for p, ws := range pwm.peerWants {
		b.WriteString(fmt.Sprintf("Peer %s: %d want-have / %d want-block:\n", p, ws.wantHaves.Len(), ws.wantBlocks.Len()))
		for _, c := range ws.wantHaves.Keys() {
			b.WriteString(fmt.Sprintf("  want-have  %s\n", c))
		}
		for _, c := range ws.wantBlocks.Keys() {
			b.WriteString(fmt.Sprintf("  want-block %s\n", c))
		}
	}
	return b.String()
}
