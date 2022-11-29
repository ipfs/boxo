package contentrouter

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/routing/http/internal"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multihash"
)

var logger = logging.Logger("service/contentrouting")

const ttl = 24 * time.Hour

type client interface {
	Provide(context.Context, []cid.Cid, time.Duration) (time.Duration, error)
	FindProviders(context.Context, cid.Cid) ([]peer.AddrInfo, error)
	Ready(context.Context) (bool, error)
}

type contentRouter struct {
	client                client
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

func NewContentRoutingClient(c client, opts ...option) *contentRouter {
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

	_, err := c.client.Provide(ctx, []cid.Cid{key}, ttl)
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
		_, err := c.client.Provide(ctx, keys, ttl)
		return err
	}

	return internal.DoBatch(
		ctx,
		c.maxProvideBatchSize,
		c.maxProvideConcurrency,
		keys,
		func(ctx context.Context, batch []cid.Cid) error {
			_, err := c.client.Provide(ctx, batch, ttl)
			return err
		},
	)
}

// Ready is part of the existing `ProvideMany` interface, but can be used more generally to determine if the routing client
// has a working connection.
func (c *contentRouter) Ready() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ready, err := c.client.Ready(ctx)
	if err != nil {
		logger.Warnw("error checking if delegated content router is ready", "Error", err)
		return false
	}
	return ready
}

func (c *contentRouter) FindProvidersAsync(ctx context.Context, key cid.Cid, numResults int) <-chan peer.AddrInfo {
	results, err := c.client.FindProviders(ctx, key)
	if err != nil {
		logger.Warnw("error finding providers", "CID", key, "Error", err)
		ch := make(chan peer.AddrInfo)
		close(ch)
		return ch
	}

	ch := make(chan peer.AddrInfo, len(results))
	for _, r := range results {
		ch <- r
	}
	close(ch)
	return ch
}
