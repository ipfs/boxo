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

type sendMessageParams struct {
	entries []*bsmsg.Entry
	targets []peer.ID
	from    uint64
}

type connectParams struct {
	peer           peer.ID
	initialEntries []*wantlist.Entry
}

type peerMessageType int

const (
	connect peerMessageType = iota + 1
	disconnect
	getPeers
	sendMessage
)

type peerMessage struct {
	messageType peerMessageType
	params      interface{}
	resultsChan chan interface{}
}

type PeerQueue interface {
	RefIncrement()
	RefDecrement() bool
	AddMessage(entries []*bsmsg.Entry, ses uint64)
	Startup(ctx context.Context, initialEntries []*wantlist.Entry)
	Shutdown()
}

type PeerQueueFactory func(p peer.ID) PeerQueue

type PeerManager struct {
	// sync channel for Run loop
	peerMessages chan peerMessage

	// synchronized by Run loop, only touch inside there
	peerQueues map[peer.ID]PeerQueue

	createPeerQueue PeerQueueFactory
	ctx             context.Context
	cancel          func()
}

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

func (pm *PeerManager) ConnectedPeers() []peer.ID {
	resp := make(chan interface{})
	pm.peerMessages <- peerMessage{getPeers, nil, resp}
	peers := <-resp
	return peers.([]peer.ID)
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

func (pm *PeerManager) Connected(p peer.ID, initialEntries []*wantlist.Entry) {
	select {
	case pm.peerMessages <- peerMessage{connect, connectParams{peer: p, initialEntries: initialEntries}, nil}:
	case <-pm.ctx.Done():
	}
}

func (pm *PeerManager) Disconnected(p peer.ID) {
	select {
	case pm.peerMessages <- peerMessage{disconnect, p, nil}:
	case <-pm.ctx.Done():
	}
}

func (pm *PeerManager) SendMessage(entries []*bsmsg.Entry, targets []peer.ID, from uint64) {
	select {
	case pm.peerMessages <- peerMessage{
		sendMessage,
		&sendMessageParams{entries: entries, targets: targets, from: from},
		nil,
	}:
	case <-pm.ctx.Done():
	}
}

func (pm *PeerManager) Startup() {
	go pm.run()
}

func (pm *PeerManager) Shutdown() {
	pm.cancel()
}

// TODO: use goprocess here once i trust it
func (pm *PeerManager) run() {
	// NOTE: Do not open any streams or connections from anywhere in this
	// event loop. Really, just don't do anything likely to block.
	for {
		select {
		case message := <-pm.peerMessages:
			pm.handleMessage(message)
		case <-pm.ctx.Done():
			return
		}
	}
}

func (pm *PeerManager) handleMessage(message peerMessage) {

	switch message.messageType {
	case sendMessage:
		ms := message.params.(*sendMessageParams)
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
	case connect:
		p := message.params.(connectParams)
		pm.startPeerHandler(p.peer, p.initialEntries)
	case disconnect:
		disconnectPeer := message.params.(peer.ID)
		pm.stopPeerHandler(disconnectPeer)
	case getPeers:
		peers := make([]peer.ID, 0, len(pm.peerQueues))
		for p := range pm.peerQueues {
			peers = append(peers, p)
		}
		message.resultsChan <- peers
	}
}
