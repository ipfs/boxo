package session

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru"
	bsgetter "github.com/ipfs/go-bitswap/getter"
	notifications "github.com/ipfs/go-bitswap/notifications"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	loggables "github.com/libp2p/go-libp2p-loggables"
	peer "github.com/libp2p/go-libp2p-peer"
)

const activeWantsLimit = 16

// WantManager is an interface that can be used to request blocks
// from given peers.
type WantManager interface {
	WantBlocks(ctx context.Context, ks []cid.Cid, peers []peer.ID, ses uint64)
	CancelWants(ctx context.Context, ks []cid.Cid, peers []peer.ID, ses uint64)
}

// PeerManager provides an interface for tracking and optimize peers, and
// requesting more when neccesary.
type PeerManager interface {
	FindMorePeers(context.Context, cid.Cid)
	GetOptimizedPeers() []peer.ID
	RecordPeerRequests([]peer.ID, []cid.Cid)
	RecordPeerResponse(peer.ID, cid.Cid)
}

type interestReq struct {
	c    cid.Cid
	resp chan bool
}

type blkRecv struct {
	from peer.ID
	blk  blocks.Block
}

// Session holds state for an individual bitswap transfer operation.
// This allows bitswap to make smarter decisions about who to send wantlist
// info to, and who to request blocks from.
type Session struct {
	// dependencies
	ctx context.Context
	wm  WantManager
	pm  PeerManager

	// channels
	incoming      chan blkRecv
	newReqs       chan []cid.Cid
	cancelKeys    chan []cid.Cid
	interestReqs  chan interestReq
	latencyReqs   chan chan time.Duration
	tickDelayReqs chan time.Duration

	// do not touch outside run loop
	tofetch       *cidQueue
	interest      *lru.Cache
	liveWants     map[cid.Cid]time.Time
	tick          *time.Timer
	baseTickDelay time.Duration
	latTotal      time.Duration
	fetchcnt      int

	// identifiers
	notif notifications.PubSub
	uuid  logging.Loggable
	id    uint64
}

// New creates a new bitswap session whose lifetime is bounded by the
// given context.
func New(ctx context.Context, id uint64, wm WantManager, pm PeerManager) *Session {
	s := &Session{
		liveWants:     make(map[cid.Cid]time.Time),
		newReqs:       make(chan []cid.Cid),
		cancelKeys:    make(chan []cid.Cid),
		tofetch:       newCidQueue(),
		interestReqs:  make(chan interestReq),
		latencyReqs:   make(chan chan time.Duration),
		tickDelayReqs: make(chan time.Duration),
		ctx:           ctx,
		wm:            wm,
		pm:            pm,
		incoming:      make(chan blkRecv),
		notif:         notifications.New(),
		uuid:          loggables.Uuid("GetBlockRequest"),
		baseTickDelay: time.Millisecond * 500,
		id:            id,
	}

	cache, _ := lru.New(2048)
	s.interest = cache

	go s.run(ctx)

	return s
}

// ReceiveBlockFrom receives an incoming block from the given peer.
func (s *Session) ReceiveBlockFrom(from peer.ID, blk blocks.Block) {
	select {
	case s.incoming <- blkRecv{from: from, blk: blk}:
	case <-s.ctx.Done():
	}
	ks := []cid.Cid{blk.Cid()}
	s.wm.CancelWants(s.ctx, ks, nil, s.id)

}

// InterestedIn returns true if this session is interested in the given Cid.
func (s *Session) InterestedIn(c cid.Cid) bool {
	if s.interest.Contains(c) {
		return true
	}
	// TODO: PERF: this is using a channel to guard a map access against race
	// conditions. This is definitely much slower than a mutex, though its unclear
	// if it will actually induce any noticeable slowness. This is implemented this
	// way to avoid adding a more complex set of mutexes around the liveWants map.
	// note that in the average case (where this session *is* interested in the
	// block we received) this function will not be called, as the cid will likely
	// still be in the interest cache.
	resp := make(chan bool, 1)
	select {
	case s.interestReqs <- interestReq{
		c:    c,
		resp: resp,
	}:
	case <-s.ctx.Done():
		return false
	}

	select {
	case want := <-resp:
		return want
	case <-s.ctx.Done():
		return false
	}
}

// GetBlock fetches a single block.
func (s *Session) GetBlock(parent context.Context, k cid.Cid) (blocks.Block, error) {
	return bsgetter.SyncGetBlock(parent, k, s.GetBlocks)
}

// GetBlocks fetches a set of blocks within the context of this session and
// returns a channel that found blocks will be returned on. No order is
// guaranteed on the returned blocks.
func (s *Session) GetBlocks(ctx context.Context, keys []cid.Cid) (<-chan blocks.Block, error) {
	ctx = logging.ContextWithLoggable(ctx, s.uuid)
	return bsgetter.AsyncGetBlocks(ctx, keys, s.notif,
		func(ctx context.Context, keys []cid.Cid) {
			select {
			case s.newReqs <- keys:
			case <-ctx.Done():
			case <-s.ctx.Done():
			}
		},
		func(keys []cid.Cid) {
			select {
			case s.cancelKeys <- keys:
			case <-s.ctx.Done():
			}
		},
	)
}

// GetAverageLatency returns the average latency for block requests.
func (s *Session) GetAverageLatency() time.Duration {
	resp := make(chan time.Duration)
	select {
	case s.latencyReqs <- resp:
	case <-s.ctx.Done():
		return -1 * time.Millisecond
	}

	select {
	case latency := <-resp:
		return latency
	case <-s.ctx.Done():
		return -1 * time.Millisecond
	}
}

// SetBaseTickDelay changes the rate at which ticks happen.
func (s *Session) SetBaseTickDelay(baseTickDelay time.Duration) {
	select {
	case s.tickDelayReqs <- baseTickDelay:
	case <-s.ctx.Done():
	}
}

const provSearchDelay = time.Second * 10

// Session run loop -- everything function below here should not be called
// of this loop
func (s *Session) run(ctx context.Context) {
	s.tick = time.NewTimer(provSearchDelay)
	for {
		select {
		case blk := <-s.incoming:
			s.handleIncomingBlock(ctx, blk)
		case keys := <-s.newReqs:
			s.handleNewRequest(ctx, keys)
		case keys := <-s.cancelKeys:
			s.handleCancel(keys)
		case <-s.tick.C:
			s.handleTick(ctx)
		case lwchk := <-s.interestReqs:
			lwchk.resp <- s.cidIsWanted(lwchk.c)
		case resp := <-s.latencyReqs:
			resp <- s.averageLatency()
		case baseTickDelay := <-s.tickDelayReqs:
			s.baseTickDelay = baseTickDelay
		case <-ctx.Done():
			s.handleShutdown()
			return
		}
	}
}

func (s *Session) handleIncomingBlock(ctx context.Context, blk blkRecv) {
	s.tick.Stop()

	if blk.from != "" {
		s.pm.RecordPeerResponse(blk.from, blk.blk.Cid())
	}

	s.receiveBlock(ctx, blk.blk)

	s.resetTick()
}

func (s *Session) handleNewRequest(ctx context.Context, keys []cid.Cid) {
	for _, k := range keys {
		s.interest.Add(k, nil)
	}
	if len(s.liveWants) < activeWantsLimit {
		toadd := activeWantsLimit - len(s.liveWants)
		if toadd > len(keys) {
			toadd = len(keys)
		}

		now := keys[:toadd]
		keys = keys[toadd:]

		s.wantBlocks(ctx, now)
	}
	for _, k := range keys {
		s.tofetch.Push(k)
	}
}

func (s *Session) handleCancel(keys []cid.Cid) {
	for _, c := range keys {
		s.tofetch.Remove(c)
	}
}

func (s *Session) handleTick(ctx context.Context) {
	live := make([]cid.Cid, 0, len(s.liveWants))
	now := time.Now()
	for c := range s.liveWants {
		live = append(live, c)
		s.liveWants[c] = now
	}

	// Broadcast these keys to everyone we're connected to
	s.pm.RecordPeerRequests(nil, live)
	s.wm.WantBlocks(ctx, live, nil, s.id)

	if len(live) > 0 {
		s.pm.FindMorePeers(ctx, live[0])
	}
	s.resetTick()
}

func (s *Session) handleShutdown() {
	s.tick.Stop()
	s.notif.Shutdown()

	live := make([]cid.Cid, 0, len(s.liveWants))
	for c := range s.liveWants {
		live = append(live, c)
	}
	s.wm.CancelWants(s.ctx, live, nil, s.id)
}

func (s *Session) cidIsWanted(c cid.Cid) bool {
	_, ok := s.liveWants[c]
	if !ok {
		ok = s.tofetch.Has(c)
	}
	return ok
}

func (s *Session) receiveBlock(ctx context.Context, blk blocks.Block) {
	c := blk.Cid()
	if s.cidIsWanted(c) {
		tval, ok := s.liveWants[c]
		if ok {
			s.latTotal += time.Since(tval)
			delete(s.liveWants, c)
		} else {
			s.tofetch.Remove(c)
		}
		s.fetchcnt++
		s.notif.Publish(blk)

		if next := s.tofetch.Pop(); next.Defined() {
			s.wantBlocks(ctx, []cid.Cid{next})
		}
	}
}

func (s *Session) wantBlocks(ctx context.Context, ks []cid.Cid) {
	now := time.Now()
	for _, c := range ks {
		s.liveWants[c] = now
	}
	peers := s.pm.GetOptimizedPeers()
	// right now we're requesting each block from every peer, but soon, maybe not
	s.pm.RecordPeerRequests(peers, ks)
	s.wm.WantBlocks(ctx, ks, peers, s.id)
}

func (s *Session) averageLatency() time.Duration {
	return s.latTotal / time.Duration(s.fetchcnt)
}

func (s *Session) resetTick() {
	if s.latTotal == 0 {
		s.tick.Reset(provSearchDelay)
	} else {
		avLat := s.averageLatency()
		s.tick.Reset(s.baseTickDelay + (3 * avLat))
	}
}
