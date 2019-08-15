package sessionmanager

import (
	"context"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	delay "github.com/ipfs/go-ipfs-delay"

	notifications "github.com/ipfs/go-bitswap/notifications"
	bssession "github.com/ipfs/go-bitswap/session"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Session is a session that is managed by the session manager
type Session interface {
	exchange.Fetcher
	InterestedIn(cid.Cid) bool
	ReceiveFrom(peer.ID, []cid.Cid)
}

type sesTrk struct {
	session Session
	pm      bssession.PeerManager
	srs     bssession.RequestSplitter
}

// SessionFactory generates a new session for the SessionManager to track.
type SessionFactory func(ctx context.Context, id uint64, pm bssession.PeerManager, srs bssession.RequestSplitter, notif notifications.PubSub, provSearchDelay time.Duration, rebroadcastDelay delay.D) Session

// RequestSplitterFactory generates a new request splitter for a session.
type RequestSplitterFactory func(ctx context.Context) bssession.RequestSplitter

// PeerManagerFactory generates a new peer manager for a session.
type PeerManagerFactory func(ctx context.Context, id uint64) bssession.PeerManager

// SessionManager is responsible for creating, managing, and dispatching to
// sessions.
type SessionManager struct {
	ctx                    context.Context
	sessionFactory         SessionFactory
	peerManagerFactory     PeerManagerFactory
	requestSplitterFactory RequestSplitterFactory
	notif                  notifications.PubSub

	// Sessions
	sessLk   sync.Mutex
	sessions []sesTrk

	// Session Index
	sessIDLk sync.Mutex
	sessID   uint64
}

// New creates a new SessionManager.
func New(ctx context.Context, sessionFactory SessionFactory, peerManagerFactory PeerManagerFactory,
	requestSplitterFactory RequestSplitterFactory, notif notifications.PubSub) *SessionManager {
	return &SessionManager{
		ctx:                    ctx,
		sessionFactory:         sessionFactory,
		peerManagerFactory:     peerManagerFactory,
		requestSplitterFactory: requestSplitterFactory,
		notif:                  notif,
	}
}

// NewSession initializes a session with the given context, and adds to the
// session manager.
func (sm *SessionManager) NewSession(ctx context.Context,
	provSearchDelay time.Duration,
	rebroadcastDelay delay.D) exchange.Fetcher {
	id := sm.GetNextSessionID()
	sessionctx, cancel := context.WithCancel(ctx)

	pm := sm.peerManagerFactory(sessionctx, id)
	srs := sm.requestSplitterFactory(sessionctx)
	session := sm.sessionFactory(sessionctx, id, pm, srs, sm.notif, provSearchDelay, rebroadcastDelay)
	tracked := sesTrk{session, pm, srs}
	sm.sessLk.Lock()
	sm.sessions = append(sm.sessions, tracked)
	sm.sessLk.Unlock()
	go func() {
		defer cancel()
		select {
		case <-sm.ctx.Done():
			sm.removeSession(tracked)
		case <-ctx.Done():
			sm.removeSession(tracked)
		}
	}()

	return session
}

func (sm *SessionManager) removeSession(session sesTrk) {
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

// GetNextSessionID returns the next sequentional identifier for a session.
func (sm *SessionManager) GetNextSessionID() uint64 {
	sm.sessIDLk.Lock()
	defer sm.sessIDLk.Unlock()
	sm.sessID++
	return sm.sessID
}

// ReceiveFrom receives blocks from a peer and dispatches to interested
// sessions.
func (sm *SessionManager) ReceiveFrom(from peer.ID, ks []cid.Cid) {
	sm.sessLk.Lock()
	defer sm.sessLk.Unlock()

	// Only give each session the blocks / dups that it is interested in
	for _, s := range sm.sessions {
		sessKs := make([]cid.Cid, 0, len(ks))
		for _, k := range ks {
			if s.session.InterestedIn(k) {
				sessKs = append(sessKs, k)
			}
		}
		s.session.ReceiveFrom(from, sessKs)
	}
}
