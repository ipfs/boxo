package session

import (
	"context"
	"time"

	bsgetter "github.com/ipfs/go-bitswap/getter"
	notifications "github.com/ipfs/go-bitswap/notifications"
	bssd "github.com/ipfs/go-bitswap/sessiondata"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	delay "github.com/ipfs/go-ipfs-delay"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
	loggables "github.com/libp2p/go-libp2p-loggables"
)

const (
	broadcastLiveWantsLimit = 4
	targetedLiveWantsLimit  = 32
)

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
	GetOptimizedPeers() []bssd.OptimizedPeer
	RecordPeerRequests([]peer.ID, []cid.Cid)
	RecordPeerResponse(peer.ID, []cid.Cid)
	RecordCancels([]cid.Cid)
}

// RequestSplitter provides an interface for splitting
// a request for Cids up among peers.
type RequestSplitter interface {
	SplitRequest([]bssd.OptimizedPeer, []cid.Cid) []bssd.PartialRequest
	RecordDuplicateBlock()
	RecordUniqueBlock()
}

type opType int

const (
	opReceive opType = iota
	opWant
	opCancel
)

type op struct {
	op   opType
	from peer.ID
	keys []cid.Cid
}

// Session holds state for an individual bitswap transfer operation.
// This allows bitswap to make smarter decisions about who to send wantlist
// info to, and who to request blocks from.
type Session struct {
	// dependencies
	ctx context.Context
	wm  WantManager
	pm  PeerManager
	srs RequestSplitter

	sw sessionWants

	// channels
	incoming      chan op
	latencyReqs   chan chan time.Duration
	tickDelayReqs chan time.Duration

	// do not touch outside run loop
	idleTick            *time.Timer
	periodicSearchTimer *time.Timer
	baseTickDelay       time.Duration
	latTotal            time.Duration
	fetchcnt            int
	consecutiveTicks    int
	initialSearchDelay  time.Duration
	periodicSearchDelay delay.D
	// identifiers
	notif notifications.PubSub
	uuid  logging.Loggable
	id    uint64
}

// New creates a new bitswap session whose lifetime is bounded by the
// given context.
func New(ctx context.Context,
	id uint64,
	wm WantManager,
	pm PeerManager,
	srs RequestSplitter,
	notif notifications.PubSub,
	initialSearchDelay time.Duration,
	periodicSearchDelay delay.D) *Session {
	s := &Session{
		sw: sessionWants{
			toFetch:   newCidQueue(),
			liveWants: make(map[cid.Cid]time.Time),
			pastWants: cid.NewSet(),
		},
		latencyReqs:         make(chan chan time.Duration),
		tickDelayReqs:       make(chan time.Duration),
		ctx:                 ctx,
		wm:                  wm,
		pm:                  pm,
		srs:                 srs,
		incoming:            make(chan op, 16),
		notif:               notif,
		uuid:                loggables.Uuid("GetBlockRequest"),
		baseTickDelay:       time.Millisecond * 500,
		id:                  id,
		initialSearchDelay:  initialSearchDelay,
		periodicSearchDelay: periodicSearchDelay,
	}

	go s.run(ctx)

	return s
}

// ReceiveFrom receives incoming blocks from the given peer.
func (s *Session) ReceiveFrom(from peer.ID, ks []cid.Cid) {
	interested := s.sw.FilterInteresting(ks)
	if len(interested) == 0 {
		return
	}

	select {
	case s.incoming <- op{op: opReceive, from: from, keys: interested}:
	case <-s.ctx.Done():
	}
}

// IsWanted returns true if this session is waiting to receive the given Cid.
func (s *Session) IsWanted(c cid.Cid) bool {
	return s.sw.IsWanted(c)
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

	return bsgetter.AsyncGetBlocks(ctx, s.ctx, keys, s.notif,
		func(ctx context.Context, keys []cid.Cid) {
			select {
			case s.incoming <- op{op: opWant, keys: keys}:
			case <-ctx.Done():
			case <-s.ctx.Done():
			}
		},
		func(keys []cid.Cid) {
			select {
			case s.incoming <- op{op: opCancel, keys: keys}:
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

// Session run loop -- everything function below here should not be called
// of this loop
func (s *Session) run(ctx context.Context) {
	s.idleTick = time.NewTimer(s.initialSearchDelay)
	s.periodicSearchTimer = time.NewTimer(s.periodicSearchDelay.NextWaitTime())
	for {
		select {
		case oper := <-s.incoming:
			switch oper.op {
			case opReceive:
				s.handleReceive(ctx, oper.from, oper.keys)
			case opWant:
				s.wantBlocks(ctx, oper.keys)
			case opCancel:
				s.sw.CancelPending(oper.keys)
			default:
				panic("unhandled operation")
			}
		case <-s.idleTick.C:
			s.handleIdleTick(ctx)
		case <-s.periodicSearchTimer.C:
			s.handlePeriodicSearch(ctx)
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

func (s *Session) handleIdleTick(ctx context.Context) {
	live := s.sw.PrepareBroadcast()

	// Broadcast these keys to everyone we're connected to
	s.pm.RecordPeerRequests(nil, live)
	s.wm.WantBlocks(ctx, live, nil, s.id)

	// do no find providers on consecutive ticks
	// -- just rely on periodic search widening
	if len(live) > 0 && (s.consecutiveTicks == 0) {
		s.pm.FindMorePeers(ctx, live[0])
	}
	s.resetIdleTick()

	if s.sw.HasLiveWants() {
		s.consecutiveTicks++
	}
}

func (s *Session) handlePeriodicSearch(ctx context.Context) {
	randomWant := s.sw.RandomLiveWant()
	if !randomWant.Defined() {
		return
	}

	// TODO: come up with a better strategy for determining when to search
	// for new providers for blocks.
	s.pm.FindMorePeers(ctx, randomWant)
	s.wm.WantBlocks(ctx, []cid.Cid{randomWant}, nil, s.id)

	s.periodicSearchTimer.Reset(s.periodicSearchDelay.NextWaitTime())
}

func (s *Session) handleShutdown() {
	s.idleTick.Stop()

	live := s.sw.LiveWants()
	s.wm.CancelWants(s.ctx, live, nil, s.id)
}

func (s *Session) handleReceive(ctx context.Context, from peer.ID, keys []cid.Cid) {
	// Record statistics only if the blocks came from the network
	// (blocks can also be received from the local node)
	if from != "" {
		s.updateReceiveCounters(ctx, from, keys)
	}

	// Update the want list
	wanted, totalLatency := s.sw.BlocksReceived(keys)
	if len(wanted) == 0 {
		return
	}

	// We've received the blocks so we can cancel any outstanding wants for them
	s.cancelIncoming(ctx, wanted)

	s.idleTick.Stop()

	// Process the received blocks
	s.processReceive(ctx, wanted, totalLatency)

	s.resetIdleTick()
}

func (s *Session) updateReceiveCounters(ctx context.Context, from peer.ID, keys []cid.Cid) {
	// Record unique vs duplicate blocks
	s.sw.ForEachUniqDup(keys, s.srs.RecordUniqueBlock, s.srs.RecordDuplicateBlock)

	// Record response (to be able to time latency)
	if len(keys) > 0 {
		s.pm.RecordPeerResponse(from, keys)
	}
}

func (s *Session) cancelIncoming(ctx context.Context, ks []cid.Cid) {
	s.pm.RecordCancels(ks)
	s.wm.CancelWants(s.ctx, ks, nil, s.id)
}

func (s *Session) processReceive(ctx context.Context, ks []cid.Cid, totalLatency time.Duration) {
	// Keep track of the total number of blocks received and total latency
	s.fetchcnt += len(ks)
	s.latTotal += totalLatency

	// We've received new wanted blocks, so reset the number of ticks
	// that have occurred since the last new block
	s.consecutiveTicks = 0

	s.wantBlocks(ctx, nil)
}

func (s *Session) wantBlocks(ctx context.Context, newks []cid.Cid) {
	// Given the want limit and any newly received blocks, get as many wants as
	// we can to send out
	ks := s.sw.GetNextWants(s.wantLimit(), newks)
	if len(ks) == 0 {
		return
	}

	peers := s.pm.GetOptimizedPeers()
	if len(peers) > 0 {
		splitRequests := s.srs.SplitRequest(peers, ks)
		for _, splitRequest := range splitRequests {
			s.pm.RecordPeerRequests(splitRequest.Peers, splitRequest.Keys)
			s.wm.WantBlocks(ctx, splitRequest.Keys, splitRequest.Peers, s.id)
		}
	} else {
		s.pm.RecordPeerRequests(nil, ks)
		s.wm.WantBlocks(ctx, ks, nil, s.id)
	}
}

func (s *Session) averageLatency() time.Duration {
	return s.latTotal / time.Duration(s.fetchcnt)
}

func (s *Session) resetIdleTick() {
	var tickDelay time.Duration
	if s.latTotal == 0 {
		tickDelay = s.initialSearchDelay
	} else {
		avLat := s.averageLatency()
		tickDelay = s.baseTickDelay + (3 * avLat)
	}
	tickDelay = tickDelay * time.Duration(1+s.consecutiveTicks)
	s.idleTick.Reset(tickDelay)
}

func (s *Session) wantLimit() int {
	if len(s.pm.GetOptimizedPeers()) > 0 {
		return targetedLiveWantsLimit
	}
	return broadcastLiveWantsLimit
}
