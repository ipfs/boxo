package sessiondata

import (
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// OptimizedPeer describes a peer and its level of optimization from 0 to 1.
type OptimizedPeer struct {
	Peer               peer.ID
	OptimizationRating float64
}

// PartialRequest is represents one slice of an over request split among peers
type PartialRequest struct {
	Peers []peer.ID
	Keys  []cid.Cid
}
