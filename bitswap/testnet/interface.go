package bitswap

import (
	bsnet "github.com/ipfs/go-bitswap/network"
	peer "github.com/libp2p/go-libp2p-peer"
	"github.com/libp2p/go-testutil"
)

// Network is an interface for generating bitswap network interfaces
// based on a test network.
type Network interface {
	Adapter(testutil.Identity) bsnet.BitSwapNetwork

	HasPeer(peer.ID) bool
}
