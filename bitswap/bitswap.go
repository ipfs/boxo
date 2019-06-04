// Package bitswap implements the IPFS exchange interface with the BitSwap
// bilateral exchange protocol.
package bitswap

import (
	"context"
	"errors"
	"sync"
	"time"

	bssrs "github.com/ipfs/go-bitswap/sessionrequestsplitter"
	delay "github.com/ipfs/go-ipfs-delay"

	decision "github.com/ipfs/go-bitswap/decision"
	bsgetter "github.com/ipfs/go-bitswap/getter"
	bsmsg "github.com/ipfs/go-bitswap/message"
	bsmq "github.com/ipfs/go-bitswap/messagequeue"
	bsnet "github.com/ipfs/go-bitswap/network"
	bspm "github.com/ipfs/go-bitswap/peermanager"
	bspqm "github.com/ipfs/go-bitswap/providerquerymanager"
	bssession "github.com/ipfs/go-bitswap/session"
	bssm "github.com/ipfs/go-bitswap/sessionmanager"
	bsspm "github.com/ipfs/go-bitswap/sessionpeermanager"
	bswm "github.com/ipfs/go-bitswap/wantmanager"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	logging "github.com/ipfs/go-log"
	metrics "github.com/ipfs/go-metrics-interface"
	process "github.com/jbenet/goprocess"
	procctx "github.com/jbenet/goprocess/context"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("bitswap")

var _ exchange.SessionExchange = (*Bitswap)(nil)

const (
	// these requests take at _least_ two minutes at the moment.
	provideTimeout         = time.Minute * 3
	defaultProvSearchDelay = time.Second
)

var (
	// HasBlockBufferSize is the buffer size of the channel for new blocks
	// that need to be provided. They should get pulled over by the
	// provideCollector even before they are actually provided.
	// TODO: Does this need to be this large givent that?
	HasBlockBufferSize    = 256
	provideKeysBufferSize = 2048
	provideWorkerMax      = 6

	// the 1<<18+15 is to observe old file chunks that are 1<<18 + 14 in size
	metricsBuckets = []float64{1 << 6, 1 << 10, 1 << 14, 1 << 18, 1<<18 + 15, 1 << 22}
)

// Option defines the functional option type that can be used to configure
// bitswap instances
type Option func(*Bitswap)

// ProvideEnabled is an option for enabling/disabling provide announcements
func ProvideEnabled(enabled bool) Option {
	return func(bs *Bitswap) {
		bs.provideEnabled = enabled
	}
}

// ProviderSearchDelay overwrites the global provider search delay
func ProviderSearchDelay(newProvSearchDelay time.Duration) Option {
	return func(bs *Bitswap) {
		bs.provSearchDelay = newProvSearchDelay
	}
}

// RebroadcastDelay overwrites the global provider rebroadcast delay
func RebroadcastDelay(newRebroadcastDelay delay.D) Option {
	return func(bs *Bitswap) {
		bs.rebroadcastDelay = newRebroadcastDelay
	}
}

// New initializes a BitSwap instance that communicates over the provided
// BitSwapNetwork. This function registers the returned instance as the network
// delegate. Runs until context is cancelled or bitswap.Close is called.
func New(parent context.Context, network bsnet.BitSwapNetwork,
	bstore blockstore.Blockstore, options ...Option) exchange.Interface {

	// important to use provided parent context (since it may include important
	// loggable data). It's probably not a good idea to allow bitswap to be
	// coupled to the concerns of the ipfs daemon in this way.
	//
	// FIXME(btc) Now that bitswap manages itself using a process, it probably
	// shouldn't accept a context anymore. Clients should probably use Close()
	// exclusively. We should probably find another way to share logging data
	ctx, cancelFunc := context.WithCancel(parent)
	ctx = metrics.CtxSubScope(ctx, "bitswap")
	dupHist := metrics.NewCtx(ctx, "recv_dup_blocks_bytes", "Summary of duplicate"+
		" data blocks recived").Histogram(metricsBuckets)
	allHist := metrics.NewCtx(ctx, "recv_all_blocks_bytes", "Summary of all"+
		" data blocks recived").Histogram(metricsBuckets)

	sentHistogram := metrics.NewCtx(ctx, "sent_all_blocks_bytes", "Histogram of blocks sent by"+
		" this bitswap").Histogram(metricsBuckets)

	px := process.WithTeardown(func() error {
		return nil
	})

	peerQueueFactory := func(ctx context.Context, p peer.ID) bspm.PeerQueue {
		return bsmq.New(ctx, p, network)
	}

	wm := bswm.New(ctx, bspm.New(ctx, peerQueueFactory))
	pqm := bspqm.New(ctx, network)

	sessionFactory := func(ctx context.Context, id uint64, pm bssession.PeerManager, srs bssession.RequestSplitter,
		provSearchDelay time.Duration,
		rebroadcastDelay delay.D) bssm.Session {
		return bssession.New(ctx, id, wm, pm, srs, provSearchDelay, rebroadcastDelay)
	}
	sessionPeerManagerFactory := func(ctx context.Context, id uint64) bssession.PeerManager {
		return bsspm.New(ctx, id, network.ConnectionManager(), pqm)
	}
	sessionRequestSplitterFactory := func(ctx context.Context) bssession.RequestSplitter {
		return bssrs.New(ctx)
	}

	bs := &Bitswap{
		blockstore:       bstore,
		engine:           decision.NewEngine(ctx, bstore, network.ConnectionManager()), // TODO close the engine with Close() method
		network:          network,
		process:          px,
		newBlocks:        make(chan cid.Cid, HasBlockBufferSize),
		provideKeys:      make(chan cid.Cid, provideKeysBufferSize),
		wm:               wm,
		pqm:              pqm,
		sm:               bssm.New(ctx, sessionFactory, sessionPeerManagerFactory, sessionRequestSplitterFactory),
		counters:         new(counters),
		dupMetric:        dupHist,
		allMetric:        allHist,
		sentHistogram:    sentHistogram,
		provideEnabled:   true,
		provSearchDelay:  defaultProvSearchDelay,
		rebroadcastDelay: delay.Fixed(time.Minute),
	}

	// apply functional options before starting and running bitswap
	for _, option := range options {
		option(bs)
	}

	bs.wm.Startup()
	bs.pqm.Startup()
	network.SetDelegate(bs)

	// Start up bitswaps async worker routines
	bs.startWorkers(ctx, px)

	// bind the context and process.
	// do it over here to avoid closing before all setup is done.
	go func() {
		<-px.Closing() // process closes first
		cancelFunc()
	}()
	procctx.CloseAfterContext(px, ctx) // parent cancelled first

	return bs
}

// Bitswap instances implement the bitswap protocol.
type Bitswap struct {
	// the wantlist tracks global wants for bitswap
	wm *bswm.WantManager

	// the provider query manager manages requests to find providers
	pqm *bspqm.ProviderQueryManager

	// the engine is the bit of logic that decides who to send which blocks to
	engine *decision.Engine

	// network delivers messages on behalf of the session
	network bsnet.BitSwapNetwork

	// blockstore is the local database
	// NB: ensure threadsafety
	blockstore blockstore.Blockstore

	// newBlocks is a channel for newly added blocks to be provided to the
	// network.  blocks pushed down this channel get buffered and fed to the
	// provideKeys channel later on to avoid too much network activity
	newBlocks chan cid.Cid
	// provideKeys directly feeds provide workers
	provideKeys chan cid.Cid

	process process.Process

	// Counters for various statistics
	counterLk sync.Mutex
	counters  *counters

	// Metrics interface metrics
	dupMetric     metrics.Histogram
	allMetric     metrics.Histogram
	sentHistogram metrics.Histogram

	// the sessionmanager manages tracking sessions
	sm *bssm.SessionManager

	// whether or not to make provide announcements
	provideEnabled bool

	// how long to wait before looking for providers in a session
	provSearchDelay time.Duration

	// how often to rebroadcast providing requests to find more optimized providers
	rebroadcastDelay delay.D
}

type counters struct {
	blocksRecvd    uint64
	dupBlocksRecvd uint64
	dupDataRecvd   uint64
	blocksSent     uint64
	dataSent       uint64
	dataRecvd      uint64
	messagesRecvd  uint64
}

// GetBlock attempts to retrieve a particular block from peers within the
// deadline enforced by the context.
func (bs *Bitswap) GetBlock(parent context.Context, k cid.Cid) (blocks.Block, error) {
	return bsgetter.SyncGetBlock(parent, k, bs.GetBlocks)
}

// WantlistForPeer returns the currently understood list of blocks requested by a
// given peer.
func (bs *Bitswap) WantlistForPeer(p peer.ID) []cid.Cid {
	var out []cid.Cid
	for _, e := range bs.engine.WantlistForPeer(p) {
		out = append(out, e.Cid)
	}
	return out
}

// LedgerForPeer returns aggregated data about blocks swapped and communication
// with a given peer.
func (bs *Bitswap) LedgerForPeer(p peer.ID) *decision.Receipt {
	return bs.engine.LedgerForPeer(p)
}

// GetBlocks returns a channel where the caller may receive blocks that
// correspond to the provided |keys|. Returns an error if BitSwap is unable to
// begin this request within the deadline enforced by the context.
//
// NB: Your request remains open until the context expires. To conserve
// resources, provide a context with a reasonably short deadline (ie. not one
// that lasts throughout the lifetime of the server)
func (bs *Bitswap) GetBlocks(ctx context.Context, keys []cid.Cid) (<-chan blocks.Block, error) {
	session := bs.sm.NewSession(ctx, bs.provSearchDelay, bs.rebroadcastDelay)
	return session.GetBlocks(ctx, keys)
}

// HasBlock announces the existence of a block to this bitswap service. The
// service will potentially notify its peers.
func (bs *Bitswap) HasBlock(blk blocks.Block) error {
	return bs.receiveBlockFrom(blk, "")
}

// TODO: Some of this stuff really only needs to be done when adding a block
// from the user, not when receiving it from the network.
// In case you run `git blame` on this comment, I'll save you some time: ask
// @whyrusleeping, I don't know the answers you seek.
func (bs *Bitswap) receiveBlockFrom(blk blocks.Block, from peer.ID) error {
	select {
	case <-bs.process.Closing():
		return errors.New("bitswap is closed")
	default:
	}

	err := bs.blockstore.Put(blk)
	if err != nil {
		log.Errorf("Error writing block to datastore: %s", err)
		return err
	}

	// NOTE: There exists the possiblity for a race condition here.  If a user
	// creates a node, then adds it to the dagservice while another goroutine
	// is waiting on a GetBlock for that object, they will receive a reference
	// to the same node. We should address this soon, but i'm not going to do
	// it now as it requires more thought and isnt causing immediate problems.

	bs.sm.ReceiveBlockFrom(from, blk)

	bs.engine.AddBlock(blk)

	if bs.provideEnabled {
		select {
		case bs.newBlocks <- blk.Cid():
			// send block off to be reprovided
		case <-bs.process.Closing():
			return bs.process.Close()
		}
	}
	return nil
}

// ReceiveMessage is called by the network interface when a new message is
// received.
func (bs *Bitswap) ReceiveMessage(ctx context.Context, p peer.ID, incoming bsmsg.BitSwapMessage) {
	bs.counterLk.Lock()
	bs.counters.messagesRecvd++
	bs.counterLk.Unlock()

	// This call records changes to wantlists, blocks received,
	// and number of bytes transfered.
	bs.engine.MessageReceived(p, incoming)
	// TODO: this is bad, and could be easily abused.
	// Should only track *useful* messages in ledger

	iblocks := incoming.Blocks()

	if len(iblocks) == 0 {
		return
	}

	wg := sync.WaitGroup{}
	for _, block := range iblocks {

		wg.Add(1)
		go func(b blocks.Block) { // TODO: this probably doesnt need to be a goroutine...
			defer wg.Done()

			bs.updateReceiveCounters(b)
			bs.sm.UpdateReceiveCounters(b)
			log.Debugf("got block %s from %s", b, p)

			// skip received blocks that are not in the wantlist
			if !bs.wm.IsWanted(b.Cid()) {
				return
			}

			if err := bs.receiveBlockFrom(b, p); err != nil {
				log.Warningf("ReceiveMessage recvBlockFrom error: %s", err)
			}
			log.Event(ctx, "Bitswap.GetBlockRequest.End", b.Cid())
		}(block)
	}
	wg.Wait()
}

func (bs *Bitswap) updateReceiveCounters(b blocks.Block) {
	blkLen := len(b.RawData())
	has, err := bs.blockstore.Has(b.Cid())
	if err != nil {
		log.Infof("blockstore.Has error: %s", err)
		return
	}

	bs.allMetric.Observe(float64(blkLen))
	if has {
		bs.dupMetric.Observe(float64(blkLen))
	}

	bs.counterLk.Lock()
	defer bs.counterLk.Unlock()
	c := bs.counters

	c.blocksRecvd++
	c.dataRecvd += uint64(len(b.RawData()))
	if has {
		c.dupBlocksRecvd++
		c.dupDataRecvd += uint64(blkLen)
	}
}

// PeerConnected is called by the network interface
// when a peer initiates a new connection to bitswap.
func (bs *Bitswap) PeerConnected(p peer.ID) {
	bs.wm.Connected(p)
	bs.engine.PeerConnected(p)
}

// PeerDisconnected is called by the network interface when a peer
// closes a connection
func (bs *Bitswap) PeerDisconnected(p peer.ID) {
	bs.wm.Disconnected(p)
	bs.engine.PeerDisconnected(p)
}

// ReceiveError is called by the network interface when an error happens
// at the network layer. Currently just logs error.
func (bs *Bitswap) ReceiveError(err error) {
	log.Infof("Bitswap ReceiveError: %s", err)
	// TODO log the network error
	// TODO bubble the network error up to the parent context/error logger
}

// Close is called to shutdown Bitswap
func (bs *Bitswap) Close() error {
	return bs.process.Close()
}

// GetWantlist returns the current local wantlist.
func (bs *Bitswap) GetWantlist() []cid.Cid {
	entries := bs.wm.CurrentWants()
	out := make([]cid.Cid, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Cid)
	}
	return out
}

// IsOnline is needed to match go-ipfs-exchange-interface
func (bs *Bitswap) IsOnline() bool {
	return true
}

// NewSession generates a new Bitswap session. You should use this, rather
// that calling Bitswap.GetBlocks, any time you intend to do several related
// block requests in a row. The session returned will have it's own GetBlocks
// method, but the session will use the fact that the requests are related to
// be more efficient in its requests to peers. If you are using a session
// from go-blockservice, it will create a bitswap session automatically.
func (bs *Bitswap) NewSession(ctx context.Context) exchange.Fetcher {
	return bs.sm.NewSession(ctx, bs.provSearchDelay, bs.rebroadcastDelay)
}
