package sessionpeermanager

import (
	"fmt"
	"sync"

	logging "github.com/ipfs/go-log/v2"

	peer "github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("bitswap/client/sesspeermgr")

const (
	// Connection Manager tag value for session peers. Indicates to connection
	// manager that it should keep the connection to the peer.
	sessionPeerTagValue = 5
)

// PeerTagger is an interface for tagging peers with metadata
type PeerTagger interface {
	TagPeer(peer.AddrInfo, string, int)
	UntagPeer(peer.AddrInfo, string)
	Protect(peer.AddrInfo, string)
	Unprotect(peer.AddrInfo, string) bool
}

// SessionPeerManager keeps track of peers for a session, and takes care of
// ConnectionManager tagging.
type SessionPeerManager struct {
	tagger PeerTagger
	tag    string

	id              uint64
	plk             sync.RWMutex
	peers           map[peer.ID]peer.AddrInfo
	peersDiscovered bool
}

// New creates a new SessionPeerManager
func New(id uint64, tagger PeerTagger) *SessionPeerManager {
	return &SessionPeerManager{
		id:     id,
		tag:    fmt.Sprint("bs-ses-", id),
		tagger: tagger,
		peers:  make(map[peer.ID]peer.AddrInfo),
	}
}

// AddPeer adds the peer to the SessionPeerManager.
// Returns true if the peer is a new peer, false if it already existed.
func (spm *SessionPeerManager) AddPeer(p peer.AddrInfo) bool {
	spm.plk.Lock()
	defer spm.plk.Unlock()

	// Check if the peer is a new peer
	if _, ok := spm.peers[p.ID]; ok {
		return false
	}

	spm.peers[p.ID] = p
	spm.peersDiscovered = true

	// Tag the peer with the ConnectionManager so it doesn't discard the
	// connection
	spm.tagger.TagPeer(p, spm.tag, sessionPeerTagValue)

	log.Debugw("Bitswap: Added peer to session", "session", spm.id, "peer", p, "peerCount", len(spm.peers))
	return true
}

// Protect connection to this peer from being pruned by the connection manager
func (spm *SessionPeerManager) ProtectConnection(p peer.AddrInfo) {
	spm.plk.Lock()
	defer spm.plk.Unlock()

	if _, ok := spm.peers[p.ID]; !ok {
		return
	}

	spm.tagger.Protect(p, spm.tag)
}

// RemovePeer removes the peer from the SessionPeerManager.
// Returns true if the peer was removed, false if it did not exist.
func (spm *SessionPeerManager) RemovePeer(p peer.AddrInfo) bool {
	spm.plk.Lock()
	defer spm.plk.Unlock()

	if _, ok := spm.peers[p.ID]; !ok {
		return false
	}

	delete(spm.peers, p.ID)
	spm.tagger.UntagPeer(p, spm.tag)
	spm.tagger.Unprotect(p, spm.tag)

	log.Debugw("Bitswap: removed peer from session", "session", spm.id, "peer", p, "peerCount", len(spm.peers))
	return true
}

// PeersDiscovered indicates whether peers have been discovered yet.
// Returns true once a peer has been discovered by the session (even if all
// peers are later removed from the session).
func (spm *SessionPeerManager) PeersDiscovered() bool {
	spm.plk.RLock()
	defer spm.plk.RUnlock()

	return spm.peersDiscovered
}

func (spm *SessionPeerManager) Peers() []peer.AddrInfo {
	spm.plk.RLock()
	defer spm.plk.RUnlock()

	peers := make([]peer.AddrInfo, 0, len(spm.peers))
	for _, info := range spm.peers {
		peers = append(peers, info)
	}

	return peers
}

func (spm *SessionPeerManager) HasPeers() bool {
	spm.plk.RLock()
	defer spm.plk.RUnlock()

	return len(spm.peers) > 0
}

func (spm *SessionPeerManager) HasPeer(p peer.ID) bool {
	spm.plk.RLock()
	defer spm.plk.RUnlock()

	_, ok := spm.peers[p]
	return ok
}

// Shutdown untags all the peers
func (spm *SessionPeerManager) Shutdown() {
	spm.plk.Lock()
	defer spm.plk.Unlock()

	// Untag the peers with the ConnectionManager so that it can release
	// connections to those peers
	for _, info := range spm.peers {
		spm.tagger.UntagPeer(info, spm.tag)
		spm.tagger.Unprotect(info, spm.tag)
	}
}
