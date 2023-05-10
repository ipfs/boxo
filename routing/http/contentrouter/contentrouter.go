package contentrouter

import (
	"context"
	"reflect"
	"time"

	"github.com/ipfs/boxo/routing/http/internal"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

var logger = logging.Logger("service/contentrouting")

const ttl = 24 * time.Hour

type Client interface {
	ProvideBitswap(ctx context.Context, keys []cid.Cid, ttl time.Duration) (time.Duration, error)
	FindProviders(ctx context.Context, key cid.Cid) (iter.ResultIter[types.ProviderResponse], error)
}

type contentRouter struct {
	client                Client
	maxProvideConcurrency int
	maxProvideBatchSize   int
}

var _ routing.ContentRouting = (*contentRouter)(nil)

type option func(c *contentRouter)

func WithMaxProvideConcurrency(max int) option {
	return func(c *contentRouter) {
		c.maxProvideConcurrency = max
	}
}

func WithMaxProvideBatchSize(max int) option {
	return func(c *contentRouter) {
		c.maxProvideBatchSize = max
	}
}

func NewContentRoutingClient(c Client, opts ...option) *contentRouter {
	cr := &contentRouter{
		client:                c,
		maxProvideConcurrency: 5,
		maxProvideBatchSize:   100,
	}
	for _, opt := range opts {
		opt(cr)
	}
	return cr
}

func (c *contentRouter) Provide(ctx context.Context, key cid.Cid, announce bool) error {
	// If 'true' is
	// passed, it also announces it, otherwise it is just kept in the local
	// accounting of which objects are being provided.
	if !announce {
		return nil
	}

	_, err := c.client.ProvideBitswap(ctx, []cid.Cid{key}, ttl)
	return err
}

// ProvideMany provides a set of keys to the remote delegate.
// Large sets of keys are chunked into multiple requests and sent concurrently, according to the concurrency configuration.
// TODO: implement retries through transient errors
func (c *contentRouter) ProvideMany(ctx context.Context, mhKeys []multihash.Multihash) error {
	keys := make([]cid.Cid, 0, len(mhKeys))
	for _, m := range mhKeys {
		keys = append(keys, cid.NewCidV1(cid.Raw, m))
	}

	if len(keys) <= c.maxProvideBatchSize {
		_, err := c.client.ProvideBitswap(ctx, keys, ttl)
		return err
	}

	return internal.DoBatch(
		ctx,
		c.maxProvideBatchSize,
		c.maxProvideConcurrency,
		keys,
		func(ctx context.Context, batch []cid.Cid) error {
			_, err := c.client.ProvideBitswap(ctx, batch, ttl)
			return err
		},
	)
}

// Ready is part of the existing `ProvideMany` interface.
func (c *contentRouter) Ready() bool {
	return true
}

// readProviderResponses reads bitswap records from the iterator into the given channel, dropping non-bitswap records.
func readProviderResponses(iter iter.ResultIter[types.ProviderResponse], ch chan<- peer.AddrInfo) {
	defer close(ch)
	defer iter.Close()
	for iter.Next() {
		res := iter.Val()
		if res.Err != nil {
			logger.Warnw("error iterating provider responses: %s", res.Err)
			continue
		}
		v := res.Val
		if v.GetSchema() == types.SchemaBitswap {
			result, ok := v.(*types.ReadBitswapProviderRecord)
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
	resultsIter, err := c.client.FindProviders(ctx, key)
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
