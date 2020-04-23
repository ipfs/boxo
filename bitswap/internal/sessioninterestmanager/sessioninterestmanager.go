package sessioninterestmanager

import (
	"sync"

	bsswl "github.com/ipfs/go-bitswap/internal/sessionwantlist"
	blocks "github.com/ipfs/go-block-format"

	cid "github.com/ipfs/go-cid"
)

// SessionInterestManager records the CIDs that each session is interested in.
type SessionInterestManager struct {
	lk         sync.RWMutex
	interested *bsswl.SessionWantlist
	wanted     *bsswl.SessionWantlist
}

// New initializes a new SessionInterestManager.
func New() *SessionInterestManager {
	return &SessionInterestManager{
		interested: bsswl.NewSessionWantlist(),
		wanted:     bsswl.NewSessionWantlist(),
	}
}

// When the client asks the session for blocks, the session calls
// RecordSessionInterest() with those cids.
func (sim *SessionInterestManager) RecordSessionInterest(ses uint64, ks []cid.Cid) {
	sim.lk.Lock()
	defer sim.lk.Unlock()

	sim.interested.Add(ks, ses)
	sim.wanted.Add(ks, ses)
}

// When the session shuts down it calls RemoveSessionInterest().
func (sim *SessionInterestManager) RemoveSessionInterest(ses uint64) []cid.Cid {
	sim.lk.Lock()
	defer sim.lk.Unlock()

	sim.wanted.RemoveSession(ses)
	return sim.interested.RemoveSession(ses)
}

// When the session receives blocks, it calls RemoveSessionWants().
func (sim *SessionInterestManager) RemoveSessionWants(ses uint64, wants []cid.Cid) {
	sim.lk.Lock()
	defer sim.lk.Unlock()

	sim.wanted.RemoveSessionKeys(ses, wants)
}

// The session calls FilterSessionInterested() to filter the sets of keys for
// those that the session is interested in
func (sim *SessionInterestManager) FilterSessionInterested(ses uint64, ksets ...[]cid.Cid) [][]cid.Cid {
	sim.lk.RLock()
	defer sim.lk.RUnlock()

	kres := make([][]cid.Cid, len(ksets))
	for i, ks := range ksets {
		kres[i] = sim.interested.SessionHas(ses, ks).Keys()
	}
	return kres
}

// When bitswap receives blocks it calls SplitWantedUnwanted() to discard
// unwanted blocks
func (sim *SessionInterestManager) SplitWantedUnwanted(blks []blocks.Block) ([]blocks.Block, []blocks.Block) {
	sim.lk.RLock()
	defer sim.lk.RUnlock()

	// Get the wanted block keys
	ks := make([]cid.Cid, len(blks))
	for _, b := range blks {
		ks = append(ks, b.Cid())
	}
	wantedKs := sim.wanted.Has(ks)

	// Separate the blocks into wanted and unwanted
	wantedBlks := make([]blocks.Block, 0, len(blks))
	notWantedBlks := make([]blocks.Block, 0)
	for _, b := range blks {
		if wantedKs.Has(b.Cid()) {
			wantedBlks = append(wantedBlks, b)
		} else {
			notWantedBlks = append(notWantedBlks, b)
		}
	}
	return wantedBlks, notWantedBlks
}

// When the SessionManager receives a message it calls InterestedSessions() to
// find out which sessions are interested in the message.
func (sim *SessionInterestManager) InterestedSessions(blks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) []uint64 {
	sim.lk.RLock()
	defer sim.lk.RUnlock()

	ks := make([]cid.Cid, 0, len(blks)+len(haves)+len(dontHaves))
	ks = append(ks, blks...)
	ks = append(ks, haves...)
	ks = append(ks, dontHaves...)

	return sim.interested.SessionsFor(ks)
}
