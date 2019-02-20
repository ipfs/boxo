package peermanager

import (
	"context"
	"sync"

	bsmsg "github.com/ipfs/go-bitswap/message"
	wantlist "github.com/ipfs/go-bitswap/wantlist"
	logging "github.com/ipfs/go-log"

	peer "github.com/libp2p/go-libp2p-peer"
)

var log = logging.Logger("bitswap")

var (
	metricsBuckets = []float64{1 << 6, 1 << 10, 1 << 14, 1 << 18, 1<<18 + 15, 1 << 22}
)

// PeerQueue provides a queer of messages to be sent for a single peer.
type PeerQueue interface {
	RefIncrement()
	RefDecrement() bool
	RefCount() int
	AddMessage(entries []*bsmsg.Entry, ses uint64)
	Startup(ctx context.Context)
	AddWantlist(initialEntries []*wantlist.Entry)
	Shutdown()
}

// PeerQueueFactory provides a function that will create a PeerQueue.
type PeerQueueFactory func(p peer.ID) PeerQueue

type peerMessage interface {
	handle(pm *PeerManager)
}

// PeerManager manages a pool of peers and sends messages to peers in the pool.
type PeerManager struct {
	// peerQueues -- interact through internal utility functions get/set/remove/iterate
	peerQueues   map[peer.ID]PeerQueue
	peerQueuesLk sync.RWMutex

	createPeerQueue PeerQueueFactory
	ctx             context.Context
}

// New creates a new PeerManager, given a context and a peerQueueFactory.
func New(ctx context.Context, createPeerQueue PeerQueueFactory) *PeerManager {
	return &PeerManager{
		peerQueues:      make(map[peer.ID]PeerQueue),
		createPeerQueue: createPeerQueue,
		ctx:             ctx,
	}
}

// ConnectedPeers returns a list of peers this PeerManager is managing.
func (pm *PeerManager) ConnectedPeers() []peer.ID {

	peers := make([]peer.ID, 0, len(pm.peerQueues))
	pm.iterate(func(p peer.ID, _ PeerQueue) {
		peers = append(peers, p)
	})
	return peers
}

// Connected is called to add a new peer to the pool, and send it an initial set
// of wants.
func (pm *PeerManager) Connected(p peer.ID, initialEntries []*wantlist.Entry) {
	mq := pm.getOrCreate(p)

	if mq.RefCount() == 0 {
		mq.AddWantlist(initialEntries)
	}
	mq.RefIncrement()
}

// Disconnected is called to remove a peer from the pool.
func (pm *PeerManager) Disconnected(p peer.ID) {
	pq, ok := pm.get(p)

	if !ok {
		// TODO: log error?
		return
	}

	if pq.RefDecrement() {
		return
	}

	pq.Shutdown()

	pm.remove(p)
}

// SendMessage is called to send a message to all or some peers in the pool;
// if targets is nil, it sends to all.
func (pm *PeerManager) SendMessage(entries []*bsmsg.Entry, targets []peer.ID, from uint64) {
	if len(targets) == 0 {
		pm.iterate(func(_ peer.ID, p PeerQueue) {
			p.AddMessage(entries, from)
		})
	} else {
		for _, t := range targets {
			p := pm.getOrCreate(t)
			p.AddMessage(entries, from)
		}
	}
}

func (pm *PeerManager) get(p peer.ID) (PeerQueue, bool) {
	pm.peerQueuesLk.RLock()
	pq, ok := pm.peerQueues[p]
	pm.peerQueuesLk.RUnlock()
	return pq, ok
}

func (pm *PeerManager) getOrCreate(p peer.ID) PeerQueue {
	pm.peerQueuesLk.Lock()
	pq, ok := pm.peerQueues[p]
	if !ok {
		pq = pm.createPeerQueue(p)
		pq.Startup(pm.ctx)
		pm.peerQueues[p] = pq
	}
	pm.peerQueuesLk.Unlock()
	return pq
}

func (pm *PeerManager) remove(p peer.ID) {
	pm.peerQueuesLk.Lock()
	delete(pm.peerQueues, p)
	pm.peerQueuesLk.Unlock()
}

func (pm *PeerManager) iterate(iterateFn func(peer.ID, PeerQueue)) {
	pm.peerQueuesLk.RLock()
	for p, pq := range pm.peerQueues {
		iterateFn(p, pq)
	}
	pm.peerQueuesLk.RUnlock()
}
