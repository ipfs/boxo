package peermanager

import (
	"bytes"
	"fmt"

	lu "github.com/ipfs/go-bitswap/internal/logutil"

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
		wantBlockGauge: wantBlockGauge,
	}
}

// AddPeer adds a peer whose wants we need to keep track of
func (pwm *peerWantManager) AddPeer(p peer.ID) {
	if _, ok := pwm.peerWants[p]; !ok {
		pwm.peerWants[p] = &peerWant{
			wantBlocks: cid.NewSet(),
			wantHaves:  cid.NewSet(),
		}
	}
}

// RemovePeer removes a peer and its associated wants from tracking
func (pwm *peerWantManager) RemovePeer(p peer.ID) {
	delete(pwm.peerWants, p)
}

// PrepareBroadcastWantHaves filters the list of want-haves for each peer,
// returning a map of peers to the want-haves they have not yet been sent.
func (pwm *peerWantManager) PrepareBroadcastWantHaves(wantHaves []cid.Cid) map[peer.ID][]cid.Cid {
	res := make(map[peer.ID][]cid.Cid)

	// Iterate over all known peers
	for p, pws := range pwm.peerWants {
		// Iterate over all want-haves
		for _, c := range wantHaves {
			// If the CID has not been sent as a want-block or want-have
			if !pws.wantBlocks.Has(c) && !pws.wantHaves.Has(c) {
				// Record that the CID has been sent as a want-have
				pws.wantHaves.Add(c)

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
func (pwm *peerWantManager) PrepareSendWants(p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) ([]cid.Cid, []cid.Cid) {
	resWantBlks := make([]cid.Cid, 0)
	resWantHvs := make([]cid.Cid, 0)

	// Get the existing want-blocks and want-haves for the peer
	if pws, ok := pwm.peerWants[p]; ok {
		// Iterate over the requested want-blocks
		for _, c := range wantBlocks {
			// If the want-block hasn't been sent to the peer
			if !pws.wantBlocks.Has(c) {
				// Record that the CID was sent as a want-block
				pws.wantBlocks.Add(c)

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

				// Add the CID to the results
				resWantHvs = append(resWantHvs, c)
			}
		}
	}

	return resWantBlks, resWantHvs
}

// PrepareSendCancels filters the list of cancels for each peer,
// returning a map of peers which only contains cancels for wants that have
// been sent to the peer.
func (pwm *peerWantManager) PrepareSendCancels(cancelKs []cid.Cid) map[peer.ID][]cid.Cid {
	res := make(map[peer.ID][]cid.Cid)

	// Iterate over all known peers
	for p, pws := range pwm.peerWants {
		// Iterate over all requested cancels
		for _, c := range cancelKs {
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
			}
		}
	}

	return res
}

// GetWantBlocks returns the set of all want-blocks sent to all peers
func (pwm *peerWantManager) GetWantBlocks() []cid.Cid {
	res := cid.NewSet()

	// Iterate over all known peers
	for _, pws := range pwm.peerWants {
		// Iterate over all want-blocks
		for _, c := range pws.wantBlocks.Keys() {
			// Add the CID to the results
			res.Add(c)
		}
	}

	return res.Keys()
}

// GetWantHaves returns the set of all want-haves sent to all peers
func (pwm *peerWantManager) GetWantHaves() []cid.Cid {
	res := cid.NewSet()

	// Iterate over all known peers
	for _, pws := range pwm.peerWants {
		// Iterate over all want-haves
		for _, c := range pws.wantHaves.Keys() {
			// Add the CID to the results
			res.Add(c)
		}
	}

	return res.Keys()
}

func (pwm *peerWantManager) String() string {
	var b bytes.Buffer
	for p, ws := range pwm.peerWants {
		b.WriteString(fmt.Sprintf("Peer %s: %d want-have / %d want-block:\n", lu.P(p), ws.wantHaves.Len(), ws.wantBlocks.Len()))
		for _, c := range ws.wantHaves.Keys() {
			b.WriteString(fmt.Sprintf("  want-have  %s\n", lu.C(c)))
		}
		for _, c := range ws.wantBlocks.Keys() {
			b.WriteString(fmt.Sprintf("  want-block %s\n", lu.C(c)))
		}
	}
	return b.String()
}
