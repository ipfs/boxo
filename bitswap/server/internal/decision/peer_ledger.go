package decision

import (
	wl "github.com/ipfs/boxo/bitswap/client/wantlist"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type peerLedger struct {
	// these two maps are inversions of each other
	peers map[peer.ID]map[cid.Cid]entry
	cids  map[cid.Cid]map[peer.ID]entry
	// value 0 mean no limit
	maxEntriesPerPeer int
}

type peerEntry struct {
	Peer peer.ID
	entry
}

type entry struct {
	Priority int32
	WantType pb.Message_Wantlist_WantType
}

func newPeerLedger(maxEntriesPerPeer uint) *peerLedger {
	return &peerLedger{
		peers: make(map[peer.ID]map[cid.Cid]entry),
		cids:  make(map[cid.Cid]map[peer.ID]entry),

		maxEntriesPerPeer: int(maxEntriesPerPeer),
	}
}

// Wants adds an entry to the peer ledger. If adding the entry would make the
// peer ledger exceed the maxEntriesPerPeer limit, then the entry is not added
// and false is returned.
func (l *peerLedger) Wants(p peer.ID, e wl.Entry) bool {
	cids, ok := l.peers[p]
	if !ok {
		cids = make(map[cid.Cid]entry)
		l.peers[p] = cids
	} else if l.maxEntriesPerPeer != 0 && len(cids) == l.maxEntriesPerPeer {
		if _, ok = cids[e.Cid]; !ok {
			return false // cannot add to peer ledger
		}
	}
	cids[e.Cid] = entry{e.Priority, e.WantType}

	m, ok := l.cids[e.Cid]
	if !ok {
		m = make(map[peer.ID]entry)
		l.cids[e.Cid] = m
	}
	m[p] = entry{e.Priority, e.WantType}

	return true
}

func (l *peerLedger) CancelWant(p peer.ID, k cid.Cid) bool {
	wants, ok := l.peers[p]
	if !ok {
		return false
	}
	_, had := wants[k]
	delete(wants, k)
	if len(wants) == 0 {
		delete(l.peers, p)
	}

	l.removePeerFromCid(p, k)
	return had
}

func (l *peerLedger) CancelWantWithType(p peer.ID, k cid.Cid, typ pb.Message_Wantlist_WantType) {
	wants, ok := l.peers[p]
	if !ok {
		return
	}
	e, ok := wants[k]
	if !ok {
		return
	}
	if typ == pb.Message_Wantlist_Have && e.WantType == pb.Message_Wantlist_Block {
		return
	}

	delete(wants, k)
	if len(wants) == 0 {
		delete(l.peers, p)
	}

	l.removePeerFromCid(p, k)
}

func (l *peerLedger) removePeerFromCid(p peer.ID, k cid.Cid) {
	m, ok := l.cids[k]
	if !ok {
		return
	}
	delete(m, p)
	if len(m) == 0 {
		delete(l.cids, k)
	}
}

func (l *peerLedger) Peers(k cid.Cid) []peerEntry {
	m, ok := l.cids[k]
	if !ok {
		return nil
	}
	peers := make([]peerEntry, 0, len(m))
	for p, e := range m {
		peers = append(peers, peerEntry{
			Peer:  p,
			entry: e,
		})
	}
	return peers
}

func (l *peerLedger) CollectPeerIDs() []peer.ID {
	peers := make([]peer.ID, 0, len(l.peers))
	for p := range l.peers {
		peers = append(peers, p)
	}
	return peers
}

func (l *peerLedger) WantlistSizeForPeer(p peer.ID) int {
	return len(l.peers[p])
}

func (l *peerLedger) WantlistForPeer(p peer.ID) []wl.Entry {
	cids, ok := l.peers[p]
	if !ok {
		return nil
	}

	entries := make([]wl.Entry, 0, len(l.cids))
	for c, e := range cids {
		entries = append(entries, wl.Entry{
			Cid:      c,
			Priority: e.Priority,
			WantType: e.WantType,
		})
	}
	return entries
}

// ClearPeerWantlist does not take an effort to fully erase it from memory.
// This is intended when the peer is still connected and the map capacity could
// be reused. If the memory should be freed use PeerDisconnected instead.
func (l *peerLedger) ClearPeerWantlist(p peer.ID) {
	cids, ok := l.peers[p]
	if !ok {
		return
	}

	for c := range cids {
		l.removePeerFromCid(p, c)
	}
}

func (l *peerLedger) PeerDisconnected(p peer.ID) {
	l.ClearPeerWantlist(p)
	delete(l.peers, p)
}

func (l *peerLedger) HasPeer(p peer.ID) bool {
	_, ok := l.peers[p]
	return ok
}
