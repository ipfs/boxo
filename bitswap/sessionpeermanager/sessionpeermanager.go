package sessionpeermanager

import (
	"context"
	"fmt"

	cid "github.com/ipfs/go-cid"
	ifconnmgr "github.com/libp2p/go-libp2p-interface-connmgr"
	peer "github.com/libp2p/go-libp2p-peer"
)

// PeerNetwork is an interface for finding providers and managing connections
type PeerNetwork interface {
	ConnectionManager() ifconnmgr.ConnManager
	FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.ID
}

// SessionPeerManager tracks and manages peers for a session, and provides
// the best ones to the session
type SessionPeerManager struct {
	ctx     context.Context
	network PeerNetwork
	tag     string

	newPeers chan peer.ID
	peerReqs chan chan []peer.ID

	// do not touch outside of run loop
	activePeers    map[peer.ID]struct{}
	activePeersArr []peer.ID
}

// New creates a new SessionPeerManager
func New(ctx context.Context, id uint64, network PeerNetwork) *SessionPeerManager {
	spm := &SessionPeerManager{
		ctx:         ctx,
		network:     network,
		newPeers:    make(chan peer.ID, 16),
		peerReqs:    make(chan chan []peer.ID),
		activePeers: make(map[peer.ID]struct{}),
	}

	spm.tag = fmt.Sprint("bs-ses-", id)

	go spm.run(ctx)
	return spm
}

// RecordPeerResponse records that a peer received a block, and adds to it
// the list of peers if it wasn't already added
func (spm *SessionPeerManager) RecordPeerResponse(p peer.ID, k cid.Cid) {

	// at the moment, we're just adding peers here
	// in the future, we'll actually use this to record metrics
	select {
	case spm.newPeers <- p:
	case <-spm.ctx.Done():
	}
}

// RecordPeerRequests records that a given set of peers requested the given cids
func (spm *SessionPeerManager) RecordPeerRequests(p []peer.ID, ks []cid.Cid) {
	// at the moment, we're not doing anything here
	// soon we'll use this to track latency by peer
}

// GetOptimizedPeers returns the best peers available for a session
func (spm *SessionPeerManager) GetOptimizedPeers() []peer.ID {
	// right now this just returns all peers, but soon we might return peers
	// ordered by optimization, or only a subset
	resp := make(chan []peer.ID)
	select {
	case spm.peerReqs <- resp:
	case <-spm.ctx.Done():
		return nil
	}

	select {
	case peers := <-resp:
		return peers
	case <-spm.ctx.Done():
		return nil
	}
}

// FindMorePeers attempts to find more peers for a session by searching for
// providers for the given Cid
func (spm *SessionPeerManager) FindMorePeers(ctx context.Context, c cid.Cid) {
	go func(k cid.Cid) {
		// TODO: have a task queue setup for this to:
		// - rate limit
		// - manage timeouts
		// - ensure two 'findprovs' calls for the same block don't run concurrently
		// - share peers between sessions based on interest set
		for p := range spm.network.FindProvidersAsync(ctx, k, 10) {
			spm.newPeers <- p
		}
	}(c)
}

func (spm *SessionPeerManager) run(ctx context.Context) {
	for {
		select {
		case p := <-spm.newPeers:
			spm.addActivePeer(p)
		case resp := <-spm.peerReqs:
			resp <- spm.activePeersArr
		case <-ctx.Done():
			spm.handleShutdown()
			return
		}
	}
}
func (spm *SessionPeerManager) addActivePeer(p peer.ID) {
	if _, ok := spm.activePeers[p]; !ok {
		spm.activePeers[p] = struct{}{}
		spm.activePeersArr = append(spm.activePeersArr, p)

		cmgr := spm.network.ConnectionManager()
		cmgr.TagPeer(p, spm.tag, 10)
	}
}

func (spm *SessionPeerManager) handleShutdown() {
	cmgr := spm.network.ConnectionManager()
	for _, p := range spm.activePeersArr {
		cmgr.UntagPeer(p, spm.tag)
	}
}
