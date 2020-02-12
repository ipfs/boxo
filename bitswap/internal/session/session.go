package session

import (
	"context"
	"sync"
	"time"

	// lu "github.com/ipfs/go-bitswap/internal/logutil"
	bsbpm "github.com/ipfs/go-bitswap/internal/blockpresencemanager"
	bsgetter "github.com/ipfs/go-bitswap/internal/getter"
	notifications "github.com/ipfs/go-bitswap/internal/notifications"
	bspm "github.com/ipfs/go-bitswap/internal/peermanager"
	bssim "github.com/ipfs/go-bitswap/internal/sessioninterestmanager"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	delay "github.com/ipfs/go-ipfs-delay"
	logging "github.com/ipfs/go-log"
	peer "github.com/libp2p/go-libp2p-core/peer"
	loggables "github.com/libp2p/go-libp2p-loggables"
)

var log = logging.Logger("bs:sess")

const (
	broadcastLiveWantsLimit = 64
)

// WantManager is an interface that can be used to request blocks
// from given peers.
type WantManager interface {
	// BroadcastWantHaves sends want-haves to all connected peers (used for
	// session discovery)
	BroadcastWantHaves(context.Context, uint64, []cid.Cid)
	// RemoveSession removes the session from the WantManager (when the
	// session shuts down)
	RemoveSession(context.Context, uint64)
}

// PeerManager keeps track of which sessions are interested in which peers
// and takes care of sending wants for the sessions
type PeerManager interface {
	// RegisterSession tells the PeerManager that the session is interested
	// in a peer's connection state
	RegisterSession(peer.ID, bspm.Session) bool
	// UnregisterSession tells the PeerManager that the session is no longer
	// interested in a peer's connection state
	UnregisterSession(uint64)
	// SendWants tells the PeerManager to send wants to the given peer
	SendWants(ctx context.Context, peerId peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid)
}

// PeerManager provides an interface for tracking and optimize peers, and
// requesting more when neccesary.
type SessionPeerManager interface {
	// ReceiveFrom is called when blocks and HAVEs are received from a peer.
	// It returns a boolean indicating if the peer is new to the session.
	ReceiveFrom(peerId peer.ID, blks []cid.Cid, haves []cid.Cid) bool
	// Peers returns the set of peers in the session.
	Peers() *peer.Set
	// FindMorePeers queries Content Routing to discover providers of the given cid
	FindMorePeers(context.Context, cid.Cid)
	// RecordPeerRequests records the time that a cid was requested from a peer
	RecordPeerRequests([]peer.ID, []cid.Cid)
	// RecordPeerResponse records the time that a response for a cid arrived
	// from a peer
	RecordPeerResponse(peer.ID, []cid.Cid)
	// RecordCancels records that cancels were sent for the given cids
	RecordCancels([]cid.Cid)
}

// opType is the kind of operation that is being processed by the event loop
type opType int

const (
	// Receive blocks
	opReceive opType = iota
	// Want blocks
	opWant
	// Cancel wants
	opCancel
	// Broadcast want-haves
	opBroadcast
)

type op struct {
	op   opType
	keys []cid.Cid
}

// Session holds state for an individual bitswap transfer operation.
// This allows bitswap to make smarter decisions about who to send wantlist
// info to, and who to request blocks from.
type Session struct {
	// dependencies
	ctx  context.Context
	wm   WantManager
	sprm SessionPeerManager
	sim  *bssim.SessionInterestManager

	sw  sessionWants
	sws sessionWantSender

	latencyTrkr latencyTracker

	// channels
	incoming      chan op
	tickDelayReqs chan time.Duration

	// do not touch outside run loop
	idleTick            *time.Timer
	periodicSearchTimer *time.Timer
	baseTickDelay       time.Duration
	consecutiveTicks    int
	initialSearchDelay  time.Duration
	periodicSearchDelay delay.D
	// identifiers
	notif notifications.PubSub
	uuid  logging.Loggable
	id    uint64

	self peer.ID
}

// New creates a new bitswap session whose lifetime is bounded by the
// given context.
func New(ctx context.Context,
	id uint64,
	wm WantManager,
	sprm SessionPeerManager,
	sim *bssim.SessionInterestManager,
	pm PeerManager,
	bpm *bsbpm.BlockPresenceManager,
	notif notifications.PubSub,
	initialSearchDelay time.Duration,
	periodicSearchDelay delay.D,
	self peer.ID) *Session {
	s := &Session{
		sw:                  newSessionWants(),
		tickDelayReqs:       make(chan time.Duration),
		ctx:                 ctx,
		wm:                  wm,
		sprm:                sprm,
		sim:                 sim,
		incoming:            make(chan op, 128),
		latencyTrkr:         latencyTracker{},
		notif:               notif,
		uuid:                loggables.Uuid("GetBlockRequest"),
		baseTickDelay:       time.Millisecond * 500,
		id:                  id,
		initialSearchDelay:  initialSearchDelay,
		periodicSearchDelay: periodicSearchDelay,
		self:                self,
	}
	s.sws = newSessionWantSender(ctx, id, pm, bpm, s.onWantsSent, s.onPeersExhausted)

	go s.run(ctx)

	return s
}

func (s *Session) ID() uint64 {
	return s.id
}

// ReceiveFrom receives incoming blocks from the given peer.
func (s *Session) ReceiveFrom(from peer.ID, ks []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) {
	interestedRes := s.sim.FilterSessionInterested(s.id, ks, haves, dontHaves)
	ks = interestedRes[0]
	haves = interestedRes[1]
	dontHaves = interestedRes[2]
	// s.logReceiveFrom(from, ks, haves, dontHaves)

	// Add any newly discovered peers that have blocks we're interested in to
	// the peer set
	isNewPeer := s.sprm.ReceiveFrom(from, ks, haves)

	// Record response timing only if the blocks came from the network
	// (blocks can also be received from the local node)
	if len(ks) > 0 && from != "" {
		s.sprm.RecordPeerResponse(from, ks)
	}

	// Update want potential
	s.sws.Update(from, ks, haves, dontHaves, isNewPeer)

	if len(ks) == 0 {
		return
	}

	// Record which blocks have been received and figure out the total latency
	// for fetching the blocks
	wanted, totalLatency := s.sw.BlocksReceived(ks)
	s.latencyTrkr.receiveUpdate(len(wanted), totalLatency)

	if len(wanted) == 0 {
		return
	}

	// Inform the SessionInterestManager that this session is no longer
	// expecting to receive the wanted keys
	s.sim.RemoveSessionWants(s.id, wanted)

	select {
	case s.incoming <- op{op: opReceive, keys: wanted}:
	case <-s.ctx.Done():
	}
}

// func (s *Session) logReceiveFrom(from peer.ID, interestedKs []cid.Cid, haves []cid.Cid, dontHaves []cid.Cid) {
// 	// log.Infof("Ses%d<-%s: %d blocks, %d haves, %d dont haves\n",
// 	// 	s.id, from, len(interestedKs), len(wantedHaves), len(wantedDontHaves))
// 	for _, c := range interestedKs {
// 		log.Warnf("Ses%d %s<-%s: block %s\n", s.id, lu.P(s.self), lu.P(from), lu.C(c))
// 	}
// 	for _, c := range haves {
// 		log.Warnf("Ses%d %s<-%s: HAVE %s\n", s.id, lu.P(s.self), lu.P(from), lu.C(c))
// 	}
// 	for _, c := range dontHaves {
// 		log.Warnf("Ses%d %s<-%s: DONT_HAVE %s\n", s.id, lu.P(s.self), lu.P(from), lu.C(c))
// 	}
// }

func (s *Session) onWantsSent(p peer.ID, wantBlocks []cid.Cid, wantHaves []cid.Cid) {
	allBlks := append(wantBlocks[:len(wantBlocks):len(wantBlocks)], wantHaves...)
	s.sw.WantsSent(allBlks)
	s.sprm.RecordPeerRequests([]peer.ID{p}, allBlks)
}

func (s *Session) onPeersExhausted(ks []cid.Cid) {
	select {
	case s.incoming <- op{op: opBroadcast, keys: ks}:
	case <-s.ctx.Done():
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

// SetBaseTickDelay changes the rate at which ticks happen.
func (s *Session) SetBaseTickDelay(baseTickDelay time.Duration) {
	select {
	case s.tickDelayReqs <- baseTickDelay:
	case <-s.ctx.Done():
	}
}

// Session run loop -- everything in this function should not be called
// outside of this loop
func (s *Session) run(ctx context.Context) {
	go s.sws.Run()

	s.idleTick = time.NewTimer(s.initialSearchDelay)
	s.periodicSearchTimer = time.NewTimer(s.periodicSearchDelay.NextWaitTime())
	for {
		select {
		case oper := <-s.incoming:
			switch oper.op {
			case opReceive:
				s.handleReceive(oper.keys)
			case opWant:
				s.wantBlocks(ctx, oper.keys)
			case opCancel:
				s.sw.CancelPending(oper.keys)
			case opBroadcast:
				s.handleIdleTick(ctx)
			default:
				panic("unhandled operation")
			}
		case <-s.idleTick.C:
			s.handleIdleTick(ctx)
		case <-s.periodicSearchTimer.C:
			s.handlePeriodicSearch(ctx)
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
	// log.Warnf("\n\n\n\n\nSes%d: broadcast %d keys\n\n\n\n\n", s.id, len(live))
	// log.Infof("Ses%d: broadcast %d keys\n", s.id, len(live))
	log.Warnf("Ses%d: broadcast %d keys", s.id, len(live))

	// Broadcast a want-have for the live wants to everyone we're connected to
	s.sprm.RecordPeerRequests(nil, live)
	s.wm.BroadcastWantHaves(ctx, s.id, live)

	// do not find providers on consecutive ticks
	// -- just rely on periodic search widening
	if len(live) > 0 && (s.consecutiveTicks == 0) {
		s.sprm.FindMorePeers(ctx, live[0])
	}
	s.resetIdleTick()

	// If we have live wants
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
	s.sprm.FindMorePeers(ctx, randomWant)

	s.wm.BroadcastWantHaves(ctx, s.id, []cid.Cid{randomWant})

	s.periodicSearchTimer.Reset(s.periodicSearchDelay.NextWaitTime())
}

func (s *Session) handleShutdown() {
	s.idleTick.Stop()
	s.wm.RemoveSession(s.ctx, s.id)
}

func (s *Session) handleReceive(ks []cid.Cid) {
	s.idleTick.Stop()

	// We've received new wanted blocks, so reset the number of ticks
	// that have occurred since the last new block
	s.consecutiveTicks = 0

	s.sprm.RecordCancels(ks)

	s.resetIdleTick()
}

func (s *Session) wantBlocks(ctx context.Context, newks []cid.Cid) {
	if len(newks) > 0 {
		s.sim.RecordSessionInterest(s.id, newks)
		s.sw.BlocksRequested(newks)
		s.sws.Add(newks)
	}

	// If we have discovered peers already, the SessionPotentialManager will
	// send wants to them
	if s.sprm.Peers().Size() > 0 {
		return
	}

	// No peers discovered yet, broadcast some want-haves
	ks := s.sw.GetNextWants(broadcastLiveWantsLimit)
	if len(ks) > 0 {
		log.Infof("Ses%d: No peers - broadcasting %d want HAVE requests\n", s.id, len(ks))
		s.sprm.RecordPeerRequests(nil, ks)
		s.wm.BroadcastWantHaves(ctx, s.id, ks)
	}
}

func (s *Session) resetIdleTick() {
	var tickDelay time.Duration
	if !s.latencyTrkr.hasLatency() {
		tickDelay = s.initialSearchDelay
	} else {
		avLat := s.latencyTrkr.averageLatency()
		// log.Warnf("averageLatency %s", avLat)
		tickDelay = s.baseTickDelay + (3 * avLat)
	}
	tickDelay = tickDelay * time.Duration(1+s.consecutiveTicks)
	s.idleTick.Reset(tickDelay)
}

type latencyTracker struct {
	sync.RWMutex
	totalLatency time.Duration
	count        int
}

func (lt *latencyTracker) hasLatency() bool {
	lt.RLock()
	defer lt.RUnlock()

	return lt.totalLatency > 0 && lt.count > 0
}

func (lt *latencyTracker) averageLatency() time.Duration {
	lt.RLock()
	defer lt.RUnlock()

	return lt.totalLatency / time.Duration(lt.count)
}

func (lt *latencyTracker) receiveUpdate(count int, totalLatency time.Duration) {
	lt.Lock()
	defer lt.Unlock()

	lt.totalLatency += totalLatency
	lt.count += count
}
