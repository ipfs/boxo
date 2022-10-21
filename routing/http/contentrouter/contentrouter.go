package client

import (
	"context"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/multiformats/go-multihash"
	"github.com/samber/lo"
)

var logger = logging.Logger("service/contentrouting")

type client interface {
	Provide(context.Context, []cid.Cid, time.Duration) (time.Duration, error)
	FindProviders(context.Context, cid.Cid) ([]peer.AddrInfo, error)
	Ready(context.Context) (bool, error)
}

type ContentRouter struct {
	client                client
	maxProvideConcurrency int
	maxProvideBatchSize   int
}

var _ routing.ContentRouting = (*ContentRouter)(nil)

func NewContentRoutingClient(c client) *ContentRouter {
	return &ContentRouter{
		client:                c,
		maxProvideConcurrency: 5,
		maxProvideBatchSize:   100,
	}
}

func (c *ContentRouter) Provide(ctx context.Context, key cid.Cid, announce bool) error {
	// If 'true' is
	// passed, it also announces it, otherwise it is just kept in the local
	// accounting of which objects are being provided.
	if !announce {
		return nil
	}

	_, err := c.client.Provide(ctx, []cid.Cid{key}, 24*time.Hour)
	return err
}

// ProvideMany provides a set of keys to the remote delegate.
// Large sets of keys are chunked into multiple requests and sent concurrently, according to the concurrency configuration.
// TODO: implement retries through transient errors
func (c *ContentRouter) ProvideMany(ctx context.Context, mhKeys []multihash.Multihash) error {
	keys := make([]cid.Cid, 0, len(mhKeys))
	for _, m := range mhKeys {
		keys = append(keys, cid.NewCidV1(cid.Raw, m))
	}

	ttl := 24 * time.Hour

	if len(keys) <= c.maxProvideBatchSize {
		_, err := c.client.Provide(ctx, keys, ttl)
		return err
	}

	chunks := lo.Chunk(keys, c.maxProvideBatchSize)

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	chunkChan := make(chan []cid.Cid)
	errChan := make(chan error)
	wg := sync.WaitGroup{}
	for i := 0; i < c.maxProvideConcurrency && i < len(chunks); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case chunk, ok := <-chunkChan:
					if !ok {
						return
					}
					_, err := c.client.Provide(workerCtx, chunk, ttl)
					if err != nil {
						select {
						case errChan <- err:
						case <-workerCtx.Done():
							return
						}
					}
				case <-workerCtx.Done():
					return
				}
			}
		}()
	}

	// work sender
	go func() {
		defer close(chunkChan)
		defer close(errChan)
		defer wg.Wait()
		for _, chunk := range chunks {
			select {
			case chunkChan <- chunk:
			case <-ctx.Done():
				return
			}
		}
	}()

	// receive any errors
	for {
		select {
		case err, ok := <-errChan:
			if !ok {
				// we finished without any errors, congratulations
				return nil
			}
			// short circuit on the first error we get
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Ready is part of the existing `ProvideMany` interface, but can be used more generally to determine if the routing client
// has a working connection.
func (c *ContentRouter) Ready() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ready, err := c.client.Ready(ctx)
	if err != nil {
		logger.Warnw("error checking if delegated content router is ready", "Error", err)
		return false
	}
	return ready
}

func (c *ContentRouter) FindProvidersAsync(ctx context.Context, key cid.Cid, numResults int) <-chan peer.AddrInfo {
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
