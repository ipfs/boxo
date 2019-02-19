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
	AddMessage(entries []*bsmsg.Entry, ses uint64)
	Startup(ctx context.Context, initialEntries []*wantlist.Entry, entries []*bsmsg.Entry, ses uint64)
	Shutdown()
}

// PeerQueueFactory provides a function that will create a PeerQueue.
type PeerQueueFactory func(p peer.ID) PeerQueue

type peerMessage interface {
	handle(pm *PeerManager)
}

// PeerManager manages a pool of peers and sends messages to peers in the pool.
type PeerManager struct {
	peerQueues      map[peer.ID]PeerQueue
	lk              sync.RWMutex
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
	pm.lk.RLock()
	defer pm.lk.RUnlock()

	peers := make([]peer.ID, 0, len(pm.peerQueues))
	for p := range pm.peerQueues {
		peers = append(peers, p)
	}

	return peers
}

// Connected is called to add a new peer to the pool, and send it an initial set
// of wants.
func (pm *PeerManager) Connected(p peer.ID, initialEntries []*wantlist.Entry) {
	pm.lk.Lock()
	defer pm.lk.Unlock()

	mq, ok := pm.peerQueues[p]
	if ok {
		mq.RefIncrement()
		return
	}

	mq = pm.createPeerQueue(p)
	pm.peerQueues[p] = mq
	mq.Startup(pm.ctx, initialEntries, nil, 0)
}

// Disconnected is called to remove a peer from the pool.
func (pm *PeerManager) Disconnected(p peer.ID) {
	pm.lk.Lock()
	defer pm.lk.Unlock()

	pq, ok := pm.peerQueues[p]
	if !ok {
		// TODO: log error?
		return
	}

	if pq.RefDecrement() {
		return
	}

	pq.Shutdown()
	delete(pm.peerQueues, p)
}

// SendMessage is called to send a message to all or some peers in the pool;
// if targets is nil, it sends to all.
func (pm *PeerManager) SendMessage(initialEntries []*wantlist.Entry, entries []*bsmsg.Entry, targets []peer.ID, from uint64) {
	pm.lk.Lock()
	defer pm.lk.Unlock()

	if len(targets) == 0 {
		for _, p := range pm.peerQueues {
			p.AddMessage(entries, from)
		}
	} else {
		for _, t := range targets {
			p, ok := pm.peerQueues[t]
			if !ok {
				p = pm.createPeerQueue(t)
				pm.peerQueues[t] = p
				p.Startup(pm.ctx, initialEntries, entries, from)
				// this is a "0 reference" queue because we haven't actually connected to it
				// sending the first message will cause it to connect
				p.RefDecrement()
			} else {
				p.AddMessage(entries, from)
			}
		}
	}
}
