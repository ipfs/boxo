package wantmanager

import (
	"context"
	"math"

	engine "github.com/ipfs/go-bitswap/decision"
	bsmsg "github.com/ipfs/go-bitswap/message"
	bsmq "github.com/ipfs/go-bitswap/messagequeue"
	bsnet "github.com/ipfs/go-bitswap/network"
	wantlist "github.com/ipfs/go-bitswap/wantlist"
	logging "github.com/ipfs/go-log"

	cid "github.com/ipfs/go-cid"
	metrics "github.com/ipfs/go-metrics-interface"
	peer "github.com/libp2p/go-libp2p-peer"
)

var log = logging.Logger("bitswap")

const (
	// kMaxPriority is the max priority as defined by the bitswap protocol
	kMaxPriority = math.MaxInt32
)

var (
	metricsBuckets = []float64{1 << 6, 1 << 10, 1 << 14, 1 << 18, 1<<18 + 15, 1 << 22}
)

type WantManager struct {
	// sync channels for Run loop
	incoming     chan *wantSet
	connectEvent chan peerStatus     // notification channel for peers connecting/disconnecting
	peerReqs     chan chan []peer.ID // channel to request connected peers on

	// synchronized by Run loop, only touch inside there
	peers map[peer.ID]*bsmq.MessageQueue
	wl    *wantlist.ThreadSafe
	bcwl  *wantlist.ThreadSafe

	network bsnet.BitSwapNetwork
	ctx     context.Context
	cancel  func()

	wantlistGauge metrics.Gauge
	sentHistogram metrics.Histogram
}

type peerStatus struct {
	connect bool
	peer    peer.ID
}

func New(ctx context.Context, network bsnet.BitSwapNetwork) *WantManager {
	ctx, cancel := context.WithCancel(ctx)
	wantlistGauge := metrics.NewCtx(ctx, "wantlist_total",
		"Number of items in wantlist.").Gauge()
	sentHistogram := metrics.NewCtx(ctx, "sent_all_blocks_bytes", "Histogram of blocks sent by"+
		" this bitswap").Histogram(metricsBuckets)
	return &WantManager{
		incoming:      make(chan *wantSet, 10),
		connectEvent:  make(chan peerStatus, 10),
		peerReqs:      make(chan chan []peer.ID),
		peers:         make(map[peer.ID]*bsmq.MessageQueue),
		wl:            wantlist.NewThreadSafe(),
		bcwl:          wantlist.NewThreadSafe(),
		network:       network,
		ctx:           ctx,
		cancel:        cancel,
		wantlistGauge: wantlistGauge,
		sentHistogram: sentHistogram,
	}
}

// WantBlocks adds the given cids to the wantlist, tracked by the given session
func (wm *WantManager) WantBlocks(ctx context.Context, ks []cid.Cid, peers []peer.ID, ses uint64) {
	log.Infof("want blocks: %s", ks)
	wm.addEntries(ctx, ks, peers, false, ses)
}

// CancelWants removes the given cids from the wantlist, tracked by the given session
func (wm *WantManager) CancelWants(ctx context.Context, ks []cid.Cid, peers []peer.ID, ses uint64) {
	wm.addEntries(context.Background(), ks, peers, true, ses)
}

type wantSet struct {
	entries []*bsmsg.Entry
	targets []peer.ID
	from    uint64
}

func (wm *WantManager) addEntries(ctx context.Context, ks []cid.Cid, targets []peer.ID, cancel bool, ses uint64) {
	entries := make([]*bsmsg.Entry, 0, len(ks))
	for i, k := range ks {
		entries = append(entries, &bsmsg.Entry{
			Cancel: cancel,
			Entry:  wantlist.NewRefEntry(k, kMaxPriority-i),
		})
	}
	select {
	case wm.incoming <- &wantSet{entries: entries, targets: targets, from: ses}:
	case <-wm.ctx.Done():
	case <-ctx.Done():
	}
}

func (wm *WantManager) ConnectedPeers() []peer.ID {
	resp := make(chan []peer.ID)
	wm.peerReqs <- resp
	return <-resp
}

func (wm *WantManager) SendBlocks(ctx context.Context, env *engine.Envelope) {
	// Blocks need to be sent synchronously to maintain proper backpressure
	// throughout the network stack
	defer env.Sent()

	msgSize := 0
	msg := bsmsg.New(false)
	for _, block := range env.Message.Blocks() {
		msgSize += len(block.RawData())
		msg.AddBlock(block)
		log.Infof("Sending block %s to %s", block, env.Peer)
	}

	wm.sentHistogram.Observe(float64(msgSize))
	err := wm.network.SendMessage(ctx, env.Peer, msg)
	if err != nil {
		log.Infof("sendblock error: %s", err)
	}
}

func (wm *WantManager) startPeerHandler(p peer.ID) *bsmq.MessageQueue {
	mq, ok := wm.peers[p]
	if ok {
		mq.RefIncrement()
		return nil
	}

	mq = bsmq.New(p, wm.network)
	wm.peers[p] = mq
	mq.Startup(wm.ctx, wm.bcwl.Entries())
	return mq
}

func (wm *WantManager) stopPeerHandler(p peer.ID) {
	pq, ok := wm.peers[p]
	if !ok {
		// TODO: log error?
		return
	}

	if pq.RefDecrement() {
		return
	}

	pq.Shutdown()
	delete(wm.peers, p)
}

func (wm *WantManager) Connected(p peer.ID) {
	select {
	case wm.connectEvent <- peerStatus{peer: p, connect: true}:
	case <-wm.ctx.Done():
	}
}

func (wm *WantManager) Disconnected(p peer.ID) {
	select {
	case wm.connectEvent <- peerStatus{peer: p, connect: false}:
	case <-wm.ctx.Done():
	}
}

// TODO: use goprocess here once i trust it
func (wm *WantManager) Run() {
	// NOTE: Do not open any streams or connections from anywhere in this
	// event loop. Really, just don't do anything likely to block.
	for {
		select {
		case ws := <-wm.incoming:

			// is this a broadcast or not?
			brdc := len(ws.targets) == 0

			// add changes to our wantlist
			for _, e := range ws.entries {
				if e.Cancel {
					if brdc {
						wm.bcwl.Remove(e.Cid, ws.from)
					}

					if wm.wl.Remove(e.Cid, ws.from) {
						wm.wantlistGauge.Dec()
					}
				} else {
					if brdc {
						wm.bcwl.AddEntry(e.Entry, ws.from)
					}
					if wm.wl.AddEntry(e.Entry, ws.from) {
						wm.wantlistGauge.Inc()
					}
				}
			}

			// broadcast those wantlist changes
			if len(ws.targets) == 0 {
				for _, p := range wm.peers {
					p.AddMessage(ws.entries, ws.from)
				}
			} else {
				for _, t := range ws.targets {
					p, ok := wm.peers[t]
					if !ok {
						log.Infof("tried sending wantlist change to non-partner peer: %s", t)
						continue
					}
					p.AddMessage(ws.entries, ws.from)
				}
			}

		case p := <-wm.connectEvent:
			if p.connect {
				wm.startPeerHandler(p.peer)
			} else {
				wm.stopPeerHandler(p.peer)
			}
		case req := <-wm.peerReqs:
			peers := make([]peer.ID, 0, len(wm.peers))
			for p := range wm.peers {
				peers = append(peers, p)
			}
			req <- peers
		case <-wm.ctx.Done():
			return
		}
	}
}

func (wm *WantManager) IsWanted(c cid.Cid) bool {
	_, isWanted := wm.wl.Contains(c)
	return isWanted
}

func (wm *WantManager) CurrentWants() []*wantlist.Entry {
	return wm.wl.Entries()
}

func (wm *WantManager) WantCount() int {
	return wm.wl.Len()
}
