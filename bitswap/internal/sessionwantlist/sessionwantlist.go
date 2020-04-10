package sessionwantlist

import (
	"sync"

	cid "github.com/ipfs/go-cid"
)

// The SessionWantList keeps track of which sessions want a CID
type SessionWantlist struct {
	sync.RWMutex
	wants map[cid.Cid]map[uint64]struct{}
}

func NewSessionWantlist() *SessionWantlist {
	return &SessionWantlist{
		wants: make(map[cid.Cid]map[uint64]struct{}),
	}
}

// The given session wants the keys
func (swl *SessionWantlist) Add(ks []cid.Cid, ses uint64) {
	swl.Lock()
	defer swl.Unlock()

	for _, c := range ks {
		if _, ok := swl.wants[c]; !ok {
			swl.wants[c] = make(map[uint64]struct{})
		}
		swl.wants[c][ses] = struct{}{}
	}
}

// Remove the keys for all sessions.
// Called when blocks are received.
func (swl *SessionWantlist) RemoveKeys(ks []cid.Cid) {
	swl.Lock()
	defer swl.Unlock()

	for _, c := range ks {
		delete(swl.wants, c)
	}
}

// Remove the session's wants, and return wants that are no longer wanted by
// any session.
func (swl *SessionWantlist) RemoveSession(ses uint64) []cid.Cid {
	swl.Lock()
	defer swl.Unlock()

	deletedKs := make([]cid.Cid, 0)
	for c := range swl.wants {
		delete(swl.wants[c], ses)
		if len(swl.wants[c]) == 0 {
			delete(swl.wants, c)
			deletedKs = append(deletedKs, c)
		}
	}

	return deletedKs
}

// Remove the session's wants
func (swl *SessionWantlist) RemoveSessionKeys(ses uint64, ks []cid.Cid) {
	swl.Lock()
	defer swl.Unlock()

	for _, c := range ks {
		if _, ok := swl.wants[c]; ok {
			delete(swl.wants[c], ses)
			if len(swl.wants[c]) == 0 {
				delete(swl.wants, c)
			}
		}
	}
}

// All keys wanted by all sessions
func (swl *SessionWantlist) Keys() []cid.Cid {
	swl.RLock()
	defer swl.RUnlock()

	ks := make([]cid.Cid, 0, len(swl.wants))
	for c := range swl.wants {
		ks = append(ks, c)
	}
	return ks
}

// All sessions that want the given keys
func (swl *SessionWantlist) SessionsFor(ks []cid.Cid) []uint64 {
	swl.RLock()
	defer swl.RUnlock()

	sesMap := make(map[uint64]struct{})
	for _, c := range ks {
		for s := range swl.wants[c] {
			sesMap[s] = struct{}{}
		}
	}

	ses := make([]uint64, 0, len(sesMap))
	for s := range sesMap {
		ses = append(ses, s)
	}
	return ses
}

// Filter for keys that at least one session wants
func (swl *SessionWantlist) Has(ks []cid.Cid) *cid.Set {
	swl.RLock()
	defer swl.RUnlock()

	has := cid.NewSet()
	for _, c := range ks {
		if _, ok := swl.wants[c]; ok {
			has.Add(c)
		}
	}
	return has
}

// Filter for keys that the given session wants
func (swl *SessionWantlist) SessionHas(ses uint64, ks []cid.Cid) *cid.Set {
	swl.RLock()
	defer swl.RUnlock()

	has := cid.NewSet()
	for _, c := range ks {
		if sesMap, cok := swl.wants[c]; cok {
			if _, sok := sesMap[ses]; sok {
				has.Add(c)
			}
		}
	}
	return has
}
