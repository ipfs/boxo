package iface

import (
	"context"

	"github.com/ipfs/boxo/coreiface/options"
	"github.com/ipfs/boxo/coreiface/path"

	"github.com/libp2p/go-libp2p/core/peer"
)

// RoutingAPI specifies the interface to the routing layer.
type RoutingAPI interface {
	// Get retrieves the best value for a given key.
	Get(context.Context, string) ([]byte, error)

	// Put sets a value for a given key.
	Put(ctx context.Context, key string, value []byte, opts ...options.RoutingPutOption) error

	// FindPeer queries the DHT for all of the multiaddresses associated with a Peer ID.
	FindPeer(context.Context, peer.ID) (peer.AddrInfo, error)

	// FindProviders finds peers in the DHT who can provide a specific value given a key.
	FindProviders(context.Context, path.Path, ...options.DhtFindProvidersOption) (<-chan peer.AddrInfo, error)

	// Provide announces to the network that you are providing given values.
	Provide(context.Context, path.Path, ...options.DhtProvideOption) error
}
