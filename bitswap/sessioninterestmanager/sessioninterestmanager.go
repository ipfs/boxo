package sessioninterestmanager

import (
	bsswl "github.com/ipfs/go-bitswap/sessionwantlist"
	blocks "github.com/ipfs/go-block-format"

	cid "github.com/ipfs/go-cid"
)

type SessionInterestManager struct {
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

func (sim *SessionInterestManager) RecordSessionInterest(ses uint64, ks []cid.Cid) {
	sim.interested.Add(ks, ses)
	sim.wanted.Add(ks, ses)
}

func (sim *SessionInterestManager) RemoveSessionInterest(ses uint64) []cid.Cid {
	sim.wanted.RemoveSession(ses)
	return sim.interested.RemoveSession(ses)
}

func (sim *SessionInterestManager) RemoveSessionWants(ses uint64, wants []cid.Cid) {
	sim.wanted.RemoveSessionKeys(ses, wants)
}

func (sim *SessionInterestManager) FilterSessionInterested(ses uint64, ksets ...[]cid.Cid) [][]cid.Cid {
	kres := make([][]cid.Cid, len(ksets))
	for i, ks := range ksets {
		kres[i] = sim.interested.SessionHas(ses, ks).Keys()
	}
	return kres
}

func (sim *SessionInterestManager) SplitWantedUnwanted(blks []blocks.Block) ([]blocks.Block, []blocks.Block) {
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

func (sim *SessionInterestManager) InterestedSessions(blks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) []uint64 {
	ks := make([]cid.Cid, 0, len(blks)+len(haves)+len(dontHaves))
	ks = append(ks, blks...)
	ks = append(ks, haves...)
	ks = append(ks, dontHaves...)

	return sim.interested.SessionsFor(ks)
}
