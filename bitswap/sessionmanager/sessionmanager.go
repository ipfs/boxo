package sessionmanager

import (
	"sync"

	exchange "github.com/ipfs/go-ipfs-exchange-interface"
)

type SessionManager struct {
	// Sessions
	sessLk   sync.Mutex
	sessions []exchange.Fetcher

	// Session Index
	sessIDLk sync.Mutex
	sessID   uint64
}

func New() *SessionManager {
	return &SessionManager{}
}

func (sm *SessionManager) AddSession(session exchange.Fetcher) {
	sm.sessLk.Lock()
	sm.sessions = append(sm.sessions, session)
	sm.sessLk.Unlock()
}

func (sm *SessionManager) RemoveSession(session exchange.Fetcher) {
	sm.sessLk.Lock()
	defer sm.sessLk.Unlock()
	for i := 0; i < len(sm.sessions); i++ {
		if sm.sessions[i] == session {
			sm.sessions[i] = sm.sessions[len(sm.sessions)-1]
			sm.sessions = sm.sessions[:len(sm.sessions)-1]
			return
		}
	}
}

func (sm *SessionManager) GetNextSessionID() uint64 {
	sm.sessIDLk.Lock()
	defer sm.sessIDLk.Unlock()
	sm.sessID++
	return sm.sessID
}

type IterateSessionFunc func(session exchange.Fetcher)

// IterateSessions loops through all managed sessions and applies the given
// IterateSessionFunc.
func (sm *SessionManager) IterateSessions(iterate IterateSessionFunc) {
	sm.sessLk.Lock()
	defer sm.sessLk.Unlock()

	for _, s := range sm.sessions {
		iterate(s)
	}
}
