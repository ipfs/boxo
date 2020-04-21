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
	peerWants map[peer.ID]*peerWant
	// Reverse index mapping wants to the peers that sent them. This is used
	// to speed up cancels
	wantPeers map[cid.Cid]map[peer.ID]struct{}
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
		peerWants:      make(map[peer.ID]*peerWant),
		wantPeers:      make(map[cid.Cid]map[peer.ID]struct{}),
		wantBlockGauge: wantBlockGauge,
	}
}

// AddPeer adds a peer whose wants we need to keep track of
func (pwm *peerWantManager) addPeer(p peer.ID) {
	if _, ok := pwm.peerWants[p]; !ok {
		pwm.peerWants[p] = &peerWant{
			wantBlocks: cid.NewSet(),
			wantHaves:  cid.NewSet(),
		}
	}
}

// RemovePeer removes a peer and its associated wants from tracking
func (pwm *peerWantManager) removePeer(p peer.ID) {
	pws, ok := pwm.peerWants[p]
	if !ok {
		return
	}

	pws.wantBlocks.ForEach(func(c cid.Cid) error {
		// Decrement the gauge by the number of pending want-blocks to the peer
		pwm.wantBlockGauge.Dec()
		// Clean up want-blocks from the reverse index
		pwm.reverseIndexRemove(c, p)
		return nil
	})

	// Clean up want-haves from the reverse index
	pws.wantHaves.ForEach(func(c cid.Cid) error {
		pwm.reverseIndexRemove(c, p)
		return nil
	})

	delete(pwm.peerWants, p)
}

// PrepareBroadcastWantHaves filters the list of want-haves for each peer,
// returning a map of peers to the want-haves they have not yet been sent.
func (pwm *peerWantManager) prepareBroadcastWantHaves(wantHaves []cid.Cid) map[peer.ID][]cid.Cid {
	res := make(map[peer.ID][]cid.Cid)

	// Iterate over all known peers
	for p, pws := range pwm.peerWants {
		// Iterate over all want-haves
		for _, c := range wantHaves {
			// If the CID has not been sent as a want-block or want-have
			if !pws.wantBlocks.Has(c) && !pws.wantHaves.Has(c) {
				// Record that the CID has been sent as a want-have
				pws.wantHaves.Add(c)

				// Update the reverse index
				pwm.reverseIndexAdd(c, p)

				// Add the CID to the results
				if _, ok := res[p]; !ok {
					res[p] = make([]cid.Cid, 0, 1)
				}
				res[p] = append(res[p], c)
			}
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
	res := make(map[peer.ID][]cid.Cid)

	// Iterate over all requested cancels
	for _, c := range cancelKs {
		// Iterate over peers that have sent a corresponding want
		for p := range pwm.wantPeers[c] {
			pws, ok := pwm.peerWants[p]
			if !ok {
				// Should never happen but check just in case
				log.Errorf("peerWantManager reverse index missing peer %s for key %s", p, c)
				continue
			}

			isWantBlock := pws.wantBlocks.Has(c)
			isWantHave := pws.wantHaves.Has(c)

			// If the CID was sent as a want-block, decrement the want-block count
			if isWantBlock {
				pwm.wantBlockGauge.Dec()
			}

			// If the CID was sent as a want-block or want-have
			if isWantBlock || isWantHave {
				// Remove the CID from the recorded want-blocks and want-haves
				pws.wantBlocks.Remove(c)
				pws.wantHaves.Remove(c)

				// Add the CID to the results
				if _, ok := res[p]; !ok {
					res[p] = make([]cid.Cid, 0, 1)
				}
				res[p] = append(res[p], c)

				// Update the reverse index
				pwm.reverseIndexRemove(c, p)
			}
		}
	}

	return res
}

// Add the peer to the list of peers that have sent a want with the cid
func (pwm *peerWantManager) reverseIndexAdd(c cid.Cid, p peer.ID) {
	peers, ok := pwm.wantPeers[c]
	if !ok {
		peers = make(map[peer.ID]struct{}, 1)
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
		pws.wantBlocks.ForEach(func(c cid.Cid) error {
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

	// Iterate over all known peers
	for _, pws := range pwm.peerWants {
		// Iterate over all want-haves
		pws.wantHaves.ForEach(func(c cid.Cid) error {
			// Add the CID to the results
			res.Add(c)
			return nil
		})
	}

	return res.Keys()
}

// GetWants returns the set of all wants (both want-blocks and want-haves).
func (pwm *peerWantManager) getWants() []cid.Cid {
	res := cid.NewSet()

	// Iterate over all known peers
	for _, pws := range pwm.peerWants {
		// Iterate over all want-blocks
		pws.wantBlocks.ForEach(func(c cid.Cid) error {
			// Add the CID to the results
			res.Add(c)
			return nil
		})

		// Iterate over all want-haves
		pws.wantHaves.ForEach(func(c cid.Cid) error {
			// Add the CID to the results
			res.Add(c)
			return nil
		})
	}

	return res.Keys()
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
