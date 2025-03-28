package network

import (
	"github.com/ipfs/boxo/swap"
	"github.com/libp2p/go-libp2p/core/peerstore"
)

// Deprecated: use github.com/ipfs/boxo/swap
//
// New returns a BitSwapNetwork supported by underlying IPFS host.
func New(pstore peerstore.Peerstore, bitswap BitSwapNetwork, http BitSwapNetwork) BitSwapNetwork {
	return swap.NewBitswapAndHTTPRouter(pstore, bitswap, http)
}
