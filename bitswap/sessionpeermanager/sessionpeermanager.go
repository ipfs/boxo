package sessionpeermanager

import (
	"context"
	"fmt"
	"math/rand"
	"sort"

	bssd "github.com/ipfs/go-bitswap/sessiondata"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

const (
	maxOptimizedPeers   = 32
	unoptimizedTagValue = 5  // tag value for "unoptimized" session peers.
	optimizedTagValue   = 10 // tag value for "optimized" session peers.
)

// PeerTagger is an interface for tagging peers with metadata
type PeerTagger interface {
	TagPeer(peer.ID, string, int)
	UntagPeer(p peer.ID, tag string)
}

// PeerProviderFinder is an interface for finding providers
type PeerProviderFinder interface {
	FindProvidersAsync(context.Context, cid.Cid) <-chan peer.ID
}

type peerMessage interface {
	handle(spm *SessionPeerManager)
}

// SessionPeerManager tracks and manages peers for a session, and provides
// the best ones to the session
type SessionPeerManager struct {
	ctx            context.Context
	tagger         PeerTagger
	providerFinder PeerProviderFinder
	tag            string
	id             uint64

	peerMessages chan peerMessage

	// do not touch outside of run loop
	activePeers         map[peer.ID]*peerData
	unoptimizedPeersArr []peer.ID
	optimizedPeersArr   []peer.ID
	broadcastLatency    *latencyTracker
}

// New creates a new SessionPeerManager
func New(ctx context.Context, id uint64, tagger PeerTagger, providerFinder PeerProviderFinder) *SessionPeerManager {
	spm := &SessionPeerManager{
		ctx:              ctx,
		tagger:         tagger,
		providerFinder: providerFinder,
		peerMessages:     make(chan peerMessage, 16),
		activePeers:      make(map[peer.ID]*peerData),
		broadcastLatency: newLatencyTracker(),
	}

	spm.tag = fmt.Sprint("bs-ses-", id)

	go spm.run(ctx)
	return spm
}

// RecordPeerResponse records that a peer received a block, and adds to it
// the list of peers if it wasn't already added
func (spm *SessionPeerManager) RecordPeerResponse(p peer.ID, k cid.Cid) {

	// at the moment, we're just adding peers here
	// in the future, we'll actually use this to record metrics
	select {
	case spm.peerMessages <- &peerResponseMessage{p, k}:
	case <-spm.ctx.Done():
	}
}

// RecordPeerRequests records that a given set of peers requested the given cids.
func (spm *SessionPeerManager) RecordPeerRequests(p []peer.ID, ks []cid.Cid) {
	// at the moment, we're not doing anything here
	// soon we'll use this to track latency by peer
	select {
	case spm.peerMessages <- &peerRequestMessage{p, ks}:
	case <-spm.ctx.Done():
	}
}

// GetOptimizedPeers returns the best peers available for a session, along with
// a rating for how good they are, in comparison to the best peer.
func (spm *SessionPeerManager) GetOptimizedPeers() []bssd.OptimizedPeer {
	// right now this just returns all peers, but soon we might return peers
	// ordered by optimization, or only a subset
	resp := make(chan []bssd.OptimizedPeer, 1)
	select {
	case spm.peerMessages <- &getPeersMessage{resp}:
	case <-spm.ctx.Done():
		return nil
	}

	select {
	case peers := <-resp:
		return peers
	case <-spm.ctx.Done():
		return nil
	}
}

// FindMorePeers attempts to find more peers for a session by searching for
// providers for the given Cid
func (spm *SessionPeerManager) FindMorePeers(ctx context.Context, c cid.Cid) {
	go func(k cid.Cid) {
		for p := range spm.providerFinder.FindProvidersAsync(ctx, k) {

			select {
			case spm.peerMessages <- &peerFoundMessage{p}:
			case <-ctx.Done():
			case <-spm.ctx.Done():
			}
		}
	}(c)
}

func (spm *SessionPeerManager) run(ctx context.Context) {
	for {
		select {
		case pm := <-spm.peerMessages:
			pm.handle(spm)
		case <-ctx.Done():
			spm.handleShutdown()
			return
		}
	}
}

func (spm *SessionPeerManager) tagPeer(p peer.ID, value int) {
	spm.tagger.TagPeer(p, spm.tag, value)
}

func (spm *SessionPeerManager) insertPeer(p peer.ID, data *peerData) {
	if data.hasLatency {
		insertPos := sort.Search(len(spm.optimizedPeersArr), func(i int) bool {
			return spm.activePeers[spm.optimizedPeersArr[i]].latency > data.latency
		})
		spm.optimizedPeersArr = append(spm.optimizedPeersArr[:insertPos],
			append([]peer.ID{p}, spm.optimizedPeersArr[insertPos:]...)...)
	} else {
		spm.unoptimizedPeersArr = append(spm.unoptimizedPeersArr, p)
	}
}

func (spm *SessionPeerManager) removeOptimizedPeer(p peer.ID) {
	for i := 0; i < len(spm.optimizedPeersArr); i++ {
		if spm.optimizedPeersArr[i] == p {
			spm.optimizedPeersArr = append(spm.optimizedPeersArr[:i], spm.optimizedPeersArr[i+1:]...)
			return
		}
	}
}

func (spm *SessionPeerManager) removeUnoptimizedPeer(p peer.ID) {
	for i := 0; i < len(spm.unoptimizedPeersArr); i++ {
		if spm.unoptimizedPeersArr[i] == p {
			spm.unoptimizedPeersArr[i] = spm.unoptimizedPeersArr[len(spm.unoptimizedPeersArr)-1]
			spm.unoptimizedPeersArr = spm.unoptimizedPeersArr[:len(spm.unoptimizedPeersArr)-1]
			return
		}
	}
}

type peerFoundMessage struct {
	p peer.ID
}

func (pfm *peerFoundMessage) handle(spm *SessionPeerManager) {
	p := pfm.p
	if _, ok := spm.activePeers[p]; !ok {
		spm.activePeers[p] = newPeerData()
		spm.insertPeer(p, spm.activePeers[p])
		spm.tagPeer(p, unoptimizedTagValue)
	}
}

type peerResponseMessage struct {
	p peer.ID
	k cid.Cid
}

func (prm *peerResponseMessage) handle(spm *SessionPeerManager) {
	p := prm.p
	k := prm.k
	data, ok := spm.activePeers[p]
	wasOptimized := ok && data.hasLatency
	if wasOptimized {
		spm.removeOptimizedPeer(p)
	} else {
		if ok {
			spm.removeUnoptimizedPeer(p)
		} else {
			data = newPeerData()
			spm.activePeers[p] = data
		}
	}
	fallbackLatency, hasFallbackLatency := spm.broadcastLatency.CheckDuration(k)
	data.AdjustLatency(k, hasFallbackLatency, fallbackLatency)
	var tagValue int
	if data.hasLatency {
		tagValue = optimizedTagValue
	} else {
		tagValue = unoptimizedTagValue
	}
	if !ok || wasOptimized != data.hasLatency {
		spm.tagPeer(p, tagValue)
	}
	spm.insertPeer(p, data)
}

type peerRequestMessage struct {
	peers []peer.ID
	keys  []cid.Cid
}

func (spm *SessionPeerManager) makeTimeout(p peer.ID) afterTimeoutFunc {
	return func(k cid.Cid) {
		spm.RecordPeerResponse(p, k)
	}
}

func (prm *peerRequestMessage) handle(spm *SessionPeerManager) {
	if prm.peers == nil {
		spm.broadcastLatency.SetupRequests(prm.keys, func(k cid.Cid) {})
	} else {
		for _, p := range prm.peers {
			if data, ok := spm.activePeers[p]; ok {
				data.lt.SetupRequests(prm.keys, spm.makeTimeout(p))
			}
		}
	}
}

type getPeersMessage struct {
	resp chan<- []bssd.OptimizedPeer
}

func (prm *getPeersMessage) handle(spm *SessionPeerManager) {
	randomOrder := rand.Perm(len(spm.unoptimizedPeersArr))
	maxPeers := len(spm.unoptimizedPeersArr) + len(spm.optimizedPeersArr)
	if maxPeers > maxOptimizedPeers {
		maxPeers = maxOptimizedPeers
	}
	var bestPeerLatency float64
	if len(spm.optimizedPeersArr) > 0 {
		bestPeerLatency = float64(spm.activePeers[spm.optimizedPeersArr[0]].latency)
	} else {
		bestPeerLatency = 0
	}
	optimizedPeers := make([]bssd.OptimizedPeer, 0, maxPeers)
	for i := 0; i < maxPeers; i++ {
		if i < len(spm.optimizedPeersArr) {
			p := spm.optimizedPeersArr[i]
			optimizedPeers = append(optimizedPeers, bssd.OptimizedPeer{
				Peer:               p,
				OptimizationRating: bestPeerLatency / float64(spm.activePeers[p].latency),
			})
		} else {
			p := spm.unoptimizedPeersArr[randomOrder[i-len(spm.optimizedPeersArr)]]
			optimizedPeers = append(optimizedPeers, bssd.OptimizedPeer{Peer: p, OptimizationRating: 0.0})
		}
	}
	prm.resp <- optimizedPeers
}

func (spm *SessionPeerManager) handleShutdown() {
	for p, data := range spm.activePeers {
		spm.tagger.UntagPeer(p, spm.tag)
		data.lt.Shutdown()
	}
}
