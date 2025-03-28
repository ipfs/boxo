package bsnet

import (
	"github.com/ipfs/boxo/swap/bitswap"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// Deprecated: use github.com/ipfs/boxo/swap/bitswap
type NetOpt bitswap.NetOpt

// Deprecated: use github.com/ipfs/boxo/swap/bitswap
type Settings bitswap.Settings

// Deprecated: use github.com/ipfs/boxo/swap/bitswap
func Prefix(prefix protocol.ID) NetOpt {
	return NetOpt(bitswap.Prefix(prefix))
}

// Deprecated: use github.com/ipfs/boxo/swap/bitswap
func SupportedProtocols(protos []protocol.ID) NetOpt {
	return NetOpt(bitswap.SupportedProtocols(protos))
}

func convertOpts(in []NetOpt) (out []bitswap.NetOpt) {
	if in == nil {
		return
	}

	out = make([]bitswap.NetOpt, len(in))
	for i, o := range in {
		out[i] = bitswap.NetOpt(o)
	}
	return
}
