package wantmanager

import (
	"context"

	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"
	bssim "github.com/ipfs/go-bitswap/internal/sessioninterestmanager"
	"github.com/ipfs/go-bitswap/internal/sessionmanager"
	bsswl "github.com/ipfs/go-bitswap/internal/sessionwantlist"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// PeerHandler sends wants / cancels to other peers
type PeerHandler interface {
	// Connected is called when a peer connects, with any initial want-haves
	// that have been broadcast to all peers (as part of session discovery)
	Connected(p peer.ID, initialWants []cid.Cid)
	// Disconnected is called when a peer disconnects
	Disconnected(p peer.ID)
	// BroadcastWantHaves sends want-haves to all connected peers
	BroadcastWantHaves(ctx context.Context, wantHaves []cid.Cid)
	// SendCancels sends cancels to all peers that had previously been sent
	// a want-block or want-have for the given key
	SendCancels(context.Context, []cid.Cid)
}

// SessionManager receives incoming messages and distributes them to sessions
type SessionManager interface {
	ReceiveFrom(p peer.ID, blks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) []sessionmanager.Session
}

// WantManager
// - informs the SessionManager and BlockPresenceManager of incoming information
//   and cancelled sessions
// - informs the PeerManager of connects and disconnects
// - manages the list of want-haves that are broadcast to the internet
//   (as opposed to being sent to specific peers)
type WantManager struct {
	bcwl *bsswl.SessionWantlist

	peerHandler PeerHandler
	sim         *bssim.SessionInterestManager
	bpm         *bsbpm.BlockPresenceManager
	sm          SessionManager
}

// New initializes a new WantManager for a given context.
func New(ctx context.Context, peerHandler PeerHandler, sim *bssim.SessionInterestManager, bpm *bsbpm.BlockPresenceManager) *WantManager {
	return &WantManager{
		bcwl:        bsswl.NewSessionWantlist(),
		peerHandler: peerHandler,
		sim:         sim,
		bpm:         bpm,
	}
}

func (wm *WantManager) SetSessionManager(sm SessionManager) {
	wm.sm = sm
}

// ReceiveFrom is called when a new message is received
func (wm *WantManager) ReceiveFrom(ctx context.Context, p peer.ID, blks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) {
	// Record block presence for HAVE / DONT_HAVE
	wm.bpm.ReceiveFrom(p, haves, dontHaves)
	// Inform interested sessions
	wm.sm.ReceiveFrom(p, blks, haves, dontHaves)
	// Remove received blocks from broadcast wantlist
	wm.bcwl.RemoveKeys(blks)
	// Send CANCEL to all peers with want-have / want-block
	wm.peerHandler.SendCancels(ctx, blks)
}

// BroadcastWantHaves is called when want-haves should be broadcast to all
// connected peers (as part of session discovery)
func (wm *WantManager) BroadcastWantHaves(ctx context.Context, ses uint64, wantHaves []cid.Cid) {
	// log.Warnf("BroadcastWantHaves session%d: %s", ses, wantHaves)

	// Record broadcast wants
	wm.bcwl.Add(wantHaves, ses)

	// Send want-haves to all peers
	wm.peerHandler.BroadcastWantHaves(ctx, wantHaves)
}

// RemoveSession is called when the session is shut down
func (wm *WantManager) RemoveSession(ctx context.Context, ses uint64) {
	// Remove session's interest in the given blocks
	cancelKs := wm.sim.RemoveSessionInterest(ses)

	// Remove broadcast want-haves for session
	wm.bcwl.RemoveSession(ses)

	// Free up block presence tracking for keys that no session is interested
	// in anymore
	wm.bpm.RemoveKeys(cancelKs)

	// Send CANCEL to all peers for blocks that no session is interested in anymore
	wm.peerHandler.SendCancels(ctx, cancelKs)
}

// Connected is called when a new peer connects
func (wm *WantManager) Connected(p peer.ID) {
	// Tell the peer handler that there is a new connection and give it the
	// list of outstanding broadcast wants
	wm.peerHandler.Connected(p, wm.bcwl.Keys())
}

// Disconnected is called when a peer disconnects
func (wm *WantManager) Disconnected(p peer.ID) {
	wm.peerHandler.Disconnected(p)
}
