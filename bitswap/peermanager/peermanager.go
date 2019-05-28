package peermanager

import (
	"context"

	bsmsg "github.com/ipfs/go-bitswap/message"
	wantlist "github.com/ipfs/go-bitswap/wantlist"

	peer "github.com/libp2p/go-libp2p-core/peer"
)

// PeerQueue provides a queer of messages to be sent for a single peer.
type PeerQueue interface {
	AddMessage(entries []bsmsg.Entry, ses uint64)
	Startup()
	AddWantlist(initialWants *wantlist.SessionTrackedWantlist)
	Shutdown()
}

// PeerQueueFactory provides a function that will create a PeerQueue.
type PeerQueueFactory func(ctx context.Context, p peer.ID) PeerQueue

type peerQueueInstance struct {
	refcnt int
	pq     PeerQueue
}

// PeerManager manages a pool of peers and sends messages to peers in the pool.
type PeerManager struct {
	// peerQueues -- interact through internal utility functions get/set/remove/iterate
	peerQueues map[peer.ID]*peerQueueInstance

	createPeerQueue PeerQueueFactory
	ctx             context.Context
}

// New creates a new PeerManager, given a context and a peerQueueFactory.
func New(ctx context.Context, createPeerQueue PeerQueueFactory) *PeerManager {
	return &PeerManager{
		peerQueues:      make(map[peer.ID]*peerQueueInstance),
		createPeerQueue: createPeerQueue,
		ctx:             ctx,
	}
}

// ConnectedPeers returns a list of peers this PeerManager is managing.
func (pm *PeerManager) ConnectedPeers() []peer.ID {
	peers := make([]peer.ID, 0, len(pm.peerQueues))
	for p := range pm.peerQueues {
		peers = append(peers, p)
	}
	return peers
}

// Connected is called to add a new peer to the pool, and send it an initial set
// of wants.
func (pm *PeerManager) Connected(p peer.ID, initialWants *wantlist.SessionTrackedWantlist) {
	pq := pm.getOrCreate(p)

	if pq.refcnt == 0 {
		pq.pq.AddWantlist(initialWants)
	}

	pq.refcnt++
}

// Disconnected is called to remove a peer from the pool.
func (pm *PeerManager) Disconnected(p peer.ID) {
	pq, ok := pm.peerQueues[p]

	if !ok {
		return
	}

	pq.refcnt--
	if pq.refcnt > 0 {
		return
	}

	delete(pm.peerQueues, p)
	pq.pq.Shutdown()
}

// SendMessage is called to send a message to all or some peers in the pool;
// if targets is nil, it sends to all.
func (pm *PeerManager) SendMessage(entries []bsmsg.Entry, targets []peer.ID, from uint64) {
	if len(targets) == 0 {
		for _, p := range pm.peerQueues {
			p.pq.AddMessage(entries, from)
		}
	} else {
		for _, t := range targets {
			pqi := pm.getOrCreate(t)
			pqi.pq.AddMessage(entries, from)
		}
	}
}

func (pm *PeerManager) getOrCreate(p peer.ID) *peerQueueInstance {
	pqi, ok := pm.peerQueues[p]
	if !ok {
		pq := pm.createPeerQueue(pm.ctx, p)
		pq.Startup()
		pqi = &peerQueueInstance{0, pq}
		pm.peerQueues[p] = pqi
	}
	return pqi
}
