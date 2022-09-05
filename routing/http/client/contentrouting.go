package client

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/multiformats/go-multihash"
)

type ContentRoutingClient struct {
	client DelegatedRoutingClient
}

var _ routing.ContentRouting = (*ContentRoutingClient)(nil)

func NewContentRoutingClient(c DelegatedRoutingClient) *ContentRoutingClient {
	return &ContentRoutingClient{client: c}
}

func (c *ContentRoutingClient) Provide(ctx context.Context, key cid.Cid, announce bool) error {
	// If 'true' is
	// passed, it also announces it, otherwise it is just kept in the local
	// accounting of which objects are being provided.
	if !announce {
		return nil
	}

	_, err := c.client.Provide(ctx, []cid.Cid{key}, 24*time.Hour)
	return err
}

func (c *ContentRoutingClient) ProvideMany(ctx context.Context, keys []multihash.Multihash) error {
	keysAsCids := make([]cid.Cid, 0, len(keys))
	for _, m := range keys {
		keysAsCids = append(keysAsCids, cid.NewCidV1(cid.Raw, m))
	}
	_, err := c.client.Provide(ctx, keysAsCids, 24*time.Hour)
	return err
}

// Ready is part of the existing `ProvideMany` interface, but can be used more generally to determine if the routing client
// has a working connection.
func (c *ContentRoutingClient) Ready() bool {
	// TODO: currently codegen does not expose a way to access the state of the connection
	// Once either that is exposed, or the `Identify` portion of the reframe spec is implemented,
	// a more nuanced response for this method will be possible.
	return true
}

func (c *ContentRoutingClient) FindProvidersAsync(ctx context.Context, key cid.Cid, numResults int) <-chan peer.AddrInfo {
	addrInfoCh := make(chan peer.AddrInfo)
	resultCh, err := c.client.FindProvidersAsync(ctx, key)
	if err != nil {
		close(addrInfoCh)
		return addrInfoCh
	}
	go func() {
		numProcessed := 0
		closed := false
		for asyncResult := range resultCh {
			if asyncResult.Err != nil {
				logger.Infof("find providers async emitted a transient error (%v)", asyncResult.Err)
			} else {
				for _, peerAddr := range asyncResult.AddrInfo {
					if numResults <= 0 || numProcessed < numResults {
						addrInfoCh <- peerAddr
					}
					numProcessed++
					if numProcessed == numResults {
						close(addrInfoCh)
						closed = true
					}
				}
			}
		}
		if !closed {
			close(addrInfoCh)
		}
	}()
	return addrInfoCh
}
