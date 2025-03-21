package bitswap

import (
	"context"

	"github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/blockexchange"
	"github.com/ipfs/boxo/swap"
	"github.com/libp2p/go-libp2p/core/routing"
)

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
type Bitswap struct {
	*blockexchange.BlockExchange
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func New(ctx context.Context, net network.BitSwapNetwork, providerFinder routing.ContentDiscovery, bstore blockstore.Blockstore, options ...Option) *Bitswap {

	bs := &Bitswap{blockexchange.New(ctx, (swap.Network)(net), providerFinder, bstore, convertOpts(options)...)}
	return bs
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
type Stat blockexchange.Stat
