package wantmanager

import (
	"context"
	"math"

	bsmsg "github.com/ipfs/go-bitswap/message"
	wantlist "github.com/ipfs/go-bitswap/wantlist"
	logging "github.com/ipfs/go-log"

	cid "github.com/ipfs/go-cid"
	metrics "github.com/ipfs/go-metrics-interface"
	peer "github.com/libp2p/go-libp2p-peer"
)

var log = logging.Logger("bitswap")

const (
	// maxPriority is the max priority as defined by the bitswap protocol
	maxPriority = math.MaxInt32
)

// WantSender sends changes out to the network as they get added to the wantlist
// managed by the WantManager
type WantSender interface {
	SendMessage(entries []*bsmsg.Entry, targets []peer.ID, from uint64)
}

type wantMessageType int

const (
	isWanted wantMessageType = iota + 1
	addWants
	currentWants
	currentBroadcastWants
	wantCount
)

type wantMessage struct {
	messageType wantMessageType
	params      interface{}
	resultsChan chan interface{}
}

// WantManager manages a global want list. It tracks two seperate want lists -
// one for all wants, and one for wants that are specifically broadcast to the
// internet
type WantManager struct {
	// channel requests to the run loop
	// to get predictable behavior while running this in a go routine
	// having only one channel is neccesary, so requests are processed serially
	messageReqs chan wantMessage

	// synchronized by Run loop, only touch inside there
	wl   *wantlist.ThreadSafe
	bcwl *wantlist.ThreadSafe

	ctx    context.Context
	cancel func()

	wantSender    WantSender
	wantlistGauge metrics.Gauge
}

// New initializes a new WantManager
func New(ctx context.Context) *WantManager {
	ctx, cancel := context.WithCancel(ctx)
	wantlistGauge := metrics.NewCtx(ctx, "wantlist_total",
		"Number of items in wantlist.").Gauge()
	return &WantManager{
		messageReqs:   make(chan wantMessage, 10),
		wl:            wantlist.NewThreadSafe(),
		bcwl:          wantlist.NewThreadSafe(),
		ctx:           ctx,
		cancel:        cancel,
		wantlistGauge: wantlistGauge,
	}
}

// SetDelegate specifies who will send want changes out to the internet
func (wm *WantManager) SetDelegate(wantSender WantSender) {
	wm.wantSender = wantSender
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
			Entry:  wantlist.NewRefEntry(k, maxPriority-i),
		})
	}
	select {
	case wm.messageReqs <- wantMessage{
		messageType: addWants,
		params:      &wantSet{entries: entries, targets: targets, from: ses},
	}:
	case <-wm.ctx.Done():
	case <-ctx.Done():
	}
}

func (wm *WantManager) Startup() {
	go wm.run()
}

func (wm *WantManager) Shutdown() {
	wm.cancel()
}

func (wm *WantManager) run() {
	// NOTE: Do not open any streams or connections from anywhere in this
	// event loop. Really, just don't do anything likely to block.
	for {
		select {
		case message := <-wm.messageReqs:
			wm.handleMessage(message)
		case <-wm.ctx.Done():
			return
		}
	}
}

func (wm *WantManager) handleMessage(message wantMessage) {
	switch message.messageType {
	case addWants:
		ws := message.params.(*wantSet)
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
		wm.wantSender.SendMessage(ws.entries, ws.targets, ws.from)
	case isWanted:
		c := message.params.(cid.Cid)
		_, isWanted := wm.wl.Contains(c)
		message.resultsChan <- isWanted
	case currentWants:
		message.resultsChan <- wm.wl.Entries()
	case currentBroadcastWants:
		message.resultsChan <- wm.bcwl.Entries()
	case wantCount:
		message.resultsChan <- wm.wl.Len()
	}
}

func (wm *WantManager) IsWanted(c cid.Cid) bool {
	resp := make(chan interface{})
	wm.messageReqs <- wantMessage{
		messageType: isWanted,
		params:      c,
		resultsChan: resp,
	}
	result := <-resp
	return result.(bool)
}

func (wm *WantManager) CurrentWants() []*wantlist.Entry {
	resp := make(chan interface{})
	wm.messageReqs <- wantMessage{
		messageType: currentWants,
		resultsChan: resp,
	}
	result := <-resp
	return result.([]*wantlist.Entry)
}

func (wm *WantManager) CurrentBroadcastWants() []*wantlist.Entry {
	resp := make(chan interface{})
	wm.messageReqs <- wantMessage{
		messageType: currentBroadcastWants,
		resultsChan: resp,
	}
	result := <-resp
	return result.([]*wantlist.Entry)
}

func (wm *WantManager) WantCount() int {
	resp := make(chan interface{})
	wm.messageReqs <- wantMessage{
		messageType: wantCount,
		resultsChan: resp,
	}
	result := <-resp
	return result.(int)
}
