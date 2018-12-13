package sessionmanager

import (
	"context"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"

	bsnet "github.com/ipfs/go-bitswap/network"
	bssession "github.com/ipfs/go-bitswap/session"
	bswm "github.com/ipfs/go-bitswap/wantmanager"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	peer "github.com/libp2p/go-libp2p-peer"
)

// SessionManager is responsible for creating, managing, and dispatching to
// sessions.
type SessionManager struct {
	wm      *bswm.WantManager
	network bsnet.BitSwapNetwork
	ctx     context.Context
	// Sessions
	sessLk   sync.Mutex
	sessions []*bssession.Session

	// Session Index
	sessIDLk sync.Mutex
	sessID   uint64
}

// New creates a new SessionManager.
func New(ctx context.Context, wm *bswm.WantManager, network bsnet.BitSwapNetwork) *SessionManager {
	return &SessionManager{
		ctx:     ctx,
		wm:      wm,
		network: network,
	}
}

// NewSession initializes a session with the given context, and adds to the
// session manager.
func (sm *SessionManager) NewSession(ctx context.Context) exchange.Fetcher {
	id := sm.GetNextSessionID()
	sessionctx, cancel := context.WithCancel(ctx)

	session := bssession.New(sessionctx, id, sm.wm, sm.network)
	sm.sessLk.Lock()
	sm.sessions = append(sm.sessions, session)
	sm.sessLk.Unlock()
	go func() {
		defer cancel()
		select {
		case <-sm.ctx.Done():
			sm.removeSession(session)
		case <-ctx.Done():
			sm.removeSession(session)
		}
	}()

	return session
}

func (sm *SessionManager) removeSession(session exchange.Fetcher) {
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

// ReceiveBlockFrom receives a block from a peer and dispatches to interested
// sessions.
func (sm *SessionManager) ReceiveBlockFrom(from peer.ID, blk blocks.Block) {
	sm.sessLk.Lock()
	defer sm.sessLk.Unlock()

	k := blk.Cid()
	ks := []cid.Cid{k}
	for _, s := range sm.sessions {
		if s.InterestedIn(k) {
			s.ReceiveBlockFrom(from, blk)
			sm.wm.CancelWants(sm.ctx, ks, nil, s.ID())
		}
	}
}
