package peermanager

import (
	"context"

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
	Startup(ctx context.Context, initialEntries []*wantlist.Entry)
	Shutdown()
}

// PeerQueueFactory provides a function that will create a PeerQueue.
type PeerQueueFactory func(p peer.ID) PeerQueue

type peerMessage interface {
	handle(pm *PeerManager)
}

// PeerManager manages a pool of peers and sends messages to peers in the pool.
type PeerManager struct {
	// sync channel for Run loop
	peerMessages chan peerMessage

	// synchronized by Run loop, only touch inside there
	peerQueues map[peer.ID]PeerQueue

	createPeerQueue PeerQueueFactory
	ctx             context.Context
	cancel          func()
}

// New creates a new PeerManager, given a context and a peerQueueFactory.
func New(ctx context.Context, createPeerQueue PeerQueueFactory) *PeerManager {
	ctx, cancel := context.WithCancel(ctx)
	return &PeerManager{
		peerMessages:    make(chan peerMessage, 10),
		peerQueues:      make(map[peer.ID]PeerQueue),
		createPeerQueue: createPeerQueue,
		ctx:             ctx,
		cancel:          cancel,
	}
}

// ConnectedPeers returns a list of peers this PeerManager is managing.
func (pm *PeerManager) ConnectedPeers() []peer.ID {
	resp := make(chan []peer.ID, 1)
	select {
	case pm.peerMessages <- &getPeersMessage{resp}:
	case <-pm.ctx.Done():
		return nil
	}
	select {
	case peers := <-resp:
		return peers
	case <-pm.ctx.Done():
		return nil
	}
}

// Connected is called to add a new peer to the pool, and send it an initial set
// of wants.
func (pm *PeerManager) Connected(p peer.ID, initialEntries []*wantlist.Entry) {
	select {
	case pm.peerMessages <- &connectPeerMessage{p, initialEntries}:
	case <-pm.ctx.Done():
	}
}

// Disconnected is called to remove a peer from the pool.
func (pm *PeerManager) Disconnected(p peer.ID) {
	select {
	case pm.peerMessages <- &disconnectPeerMessage{p}:
	case <-pm.ctx.Done():
	}
}

// SendMessage is called to send a message to all or some peers in the pool;
// if targets is nil, it sends to all.
func (pm *PeerManager) SendMessage(entries []*bsmsg.Entry, targets []peer.ID, from uint64) {
	select {
	case pm.peerMessages <- &sendPeerMessage{entries: entries, targets: targets, from: from}:
	case <-pm.ctx.Done():
	}
}

// Startup enables the run loop for the PeerManager - no processing will occur
// if startup is not called.
func (pm *PeerManager) Startup() {
	go pm.run()
}

// Shutdown shutsdown processing for the PeerManager.
func (pm *PeerManager) Shutdown() {
	pm.cancel()
}

func (pm *PeerManager) run() {
	for {
		select {
		case message := <-pm.peerMessages:
			message.handle(pm)
		case <-pm.ctx.Done():
			return
		}
	}
}

type sendPeerMessage struct {
	entries []*bsmsg.Entry
	targets []peer.ID
	from    uint64
}

func (s *sendPeerMessage) handle(pm *PeerManager) {
	pm.sendMessage(s)
}

type connectPeerMessage struct {
	p              peer.ID
	initialEntries []*wantlist.Entry
}

func (c *connectPeerMessage) handle(pm *PeerManager) {
	pm.startPeerHandler(c.p, c.initialEntries)
}

type disconnectPeerMessage struct {
	p peer.ID
}

func (dc *disconnectPeerMessage) handle(pm *PeerManager) {
	pm.stopPeerHandler(dc.p)
}

type getPeersMessage struct {
	peerResp chan<- []peer.ID
}

func (gp *getPeersMessage) handle(pm *PeerManager) {
	pm.getPeers(gp.peerResp)
}

func (pm *PeerManager) getPeers(peerResp chan<- []peer.ID) {
	peers := make([]peer.ID, 0, len(pm.peerQueues))
	for p := range pm.peerQueues {
		peers = append(peers, p)
	}
	peerResp <- peers
}

func (pm *PeerManager) startPeerHandler(p peer.ID, initialEntries []*wantlist.Entry) PeerQueue {
	mq, ok := pm.peerQueues[p]
	if ok {
		mq.RefIncrement()
		return nil
	}

	mq = pm.createPeerQueue(p)
	pm.peerQueues[p] = mq
	mq.Startup(pm.ctx, initialEntries)
	return mq
}

func (pm *PeerManager) stopPeerHandler(p peer.ID) {
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

func (pm *PeerManager) sendMessage(ms *sendPeerMessage) {
	if len(ms.targets) == 0 {
		for _, p := range pm.peerQueues {
			p.AddMessage(ms.entries, ms.from)
		}
	} else {
		for _, t := range ms.targets {
			p, ok := pm.peerQueues[t]
			if !ok {
				log.Infof("tried sending wantlist change to non-partner peer: %s", t)
				continue
			}
			p.AddMessage(ms.entries, ms.from)
		}
	}
}
