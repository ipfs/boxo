package sessionpeermanager

import (
	"context"
	"fmt"

	cid "github.com/ipfs/go-cid"
	ifconnmgr "github.com/libp2p/go-libp2p-interface-connmgr"
	peer "github.com/libp2p/go-libp2p-peer"
)

type PeerNetwork interface {
	ConnectionManager() ifconnmgr.ConnManager
	FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.ID
}

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

func (spm *SessionPeerManager) RecordPeerResponse(p peer.ID, k cid.Cid) {
	// at the moment, we're just adding peers here
	// in the future, we'll actually use this to record metrics
	select {
	case spm.newPeers <- p:
	case <-spm.ctx.Done():
	}
}

func (spm *SessionPeerManager) RecordPeerRequests(p []peer.ID, ks []cid.Cid) {
	// at the moment, we're not doing anything here
	// soon we'll use this to track latency by peer
}

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
