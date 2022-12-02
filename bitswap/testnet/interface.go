package bitswap

import (
	bsnet "github.com/ipfs/go-bitswap/network"

	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Network is an interface for generating bitswap network interfaces
// based on a test network.
type Network interface {
	Adapter(tnet.Identity, ...bsnet.NetOpt) bsnet.BitSwapNetwork

	HasPeer(peer.ID) bool
}
