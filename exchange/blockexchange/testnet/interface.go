package bitswap

import (
	"github.com/ipfs/boxo/swap"
	"github.com/ipfs/boxo/swap/bitswap"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Network is an interface for generating bitswap network interfaces
// based on a test network.
type Network interface {
	Adapter(tnet.Identity, ...bitswap.NetOpt) swap.Network

	HasPeer(peer.ID) bool
}
