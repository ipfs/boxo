package contentrouter

import (
	"context"
	"reflect"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

var logger = logging.Logger("service/contentrouting")

type Client interface {
	GetProviders(ctx context.Context, key cid.Cid) (iter.ResultIter[types.Record], error)
}

type contentRouter struct {
	client Client
}

var _ routing.ContentRouting = (*contentRouter)(nil)
var _ routinghelpers.ProvideManyRouter = (*contentRouter)(nil)

type option func(c *contentRouter)

func NewContentRoutingClient(c Client, opts ...option) *contentRouter {
	cr := &contentRouter{
		client: c,
	}
	for _, opt := range opts {
		opt(cr)
	}
	return cr
}

func (c *contentRouter) Provide(ctx context.Context, key cid.Cid, announce bool) error {
	return routing.ErrNotSupported
}

func (c *contentRouter) ProvideMany(ctx context.Context, mhKeys []multihash.Multihash) error {
	return routing.ErrNotSupported
}

// Ready is part of the existing `ProvideMany` interface.
func (c *contentRouter) Ready() bool {
	return true
}

// readProviderResponses reads bitswap records from the iterator into the given channel, dropping non-bitswap records.
func readProviderResponses(iter iter.ResultIter[types.Record], ch chan<- peer.AddrInfo) {
	defer close(ch)
	defer iter.Close()
	for iter.Next() {
		res := iter.Val()
		if res.Err != nil {
			logger.Warnw("error iterating provider responses: %s", res.Err)
			continue
		}
		v := res.Val
		if v.GetSchema() == types.SchemaPeer {
			result, ok := v.(*types.PeerRecord)
			if !ok {
				logger.Errorw(
					"problem casting find providers result",
					"Schema", v.GetSchema(),
					"Type", reflect.TypeOf(v).String(),
				)
				continue
			}

			var addrs []multiaddr.Multiaddr
			for _, a := range result.Addrs {
				addrs = append(addrs, a.Multiaddr)
			}

			ch <- peer.AddrInfo{
				ID:    *result.ID,
				Addrs: addrs,
			}
		}
	}
}

func (c *contentRouter) FindProvidersAsync(ctx context.Context, key cid.Cid, numResults int) <-chan peer.AddrInfo {
	resultsIter, err := c.client.GetProviders(ctx, key)
	if err != nil {
		logger.Warnw("error finding providers", "CID", key, "Error", err)
		ch := make(chan peer.AddrInfo)
		close(ch)
		return ch
	}
	ch := make(chan peer.AddrInfo)
	go readProviderResponses(resultsIter, ch)
	return ch
}
