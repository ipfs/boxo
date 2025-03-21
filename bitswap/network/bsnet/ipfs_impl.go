package bsnet

import (
	iface "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/swap/bitswap"

	"github.com/libp2p/go-libp2p/core/host"
)

// Deprecated: use github.com/swap/bitswap
//
// NewFromIpfsHost returns a BitSwapNetwork supported by underlying IPFS host.
func NewFromIpfsHost(host host.Host, opts ...NetOpt) iface.BitSwapNetwork {
	return bitswap.NewFromIpfsHost(host, convertOpts(opts)...)
}
