package contentrouter

import (
	"context"
	"reflect"
	"strings"

	"github.com/ipfs/boxo/ipns"
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

var logger = logging.Logger("routing/http/contentrouter")

type Client interface {
	FindProviders(ctx context.Context, key cid.Cid) (iter.ResultIter[types.Record], error)
	FindPeers(ctx context.Context, pid peer.ID) (peers iter.ResultIter[types.Record], err error)
	FindIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error)
	ProvideIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error
}

type contentRouter struct {
	client Client
}

var _ routing.ContentRouting = (*contentRouter)(nil)
var _ routing.PeerRouting = (*contentRouter)(nil)
var _ routing.ValueStore = (*contentRouter)(nil)
var _ routinghelpers.ProvideManyRouter = (*contentRouter)(nil)
var _ routinghelpers.ReadyAbleRouter = (*contentRouter)(nil)

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

// Ready is part of the existing [routing.ReadyAbleRouter] interface.
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

func (c *contentRouter) FindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	iter, err := c.client.FindPeers(ctx, pid)
	if err != nil {
		return peer.AddrInfo{}, err
	}
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

			return peer.AddrInfo{
				ID:    *result.ID,
				Addrs: addrs,
			}, nil
		}
	}

	return peer.AddrInfo{}, err
}

func (c *contentRouter) PutValue(ctx context.Context, key string, data []byte, opts ...routing.Option) error {
	if !strings.HasPrefix(key, "/ipns/") {
		return routing.ErrNotSupported
	}

	name, err := ipns.NameFromRoutingKey([]byte(key))
	if err != nil {
		return err
	}

	record, err := ipns.UnmarshalRecord(data)
	if err != nil {
		return err
	}

	return c.client.ProvideIPNS(ctx, name, record)
}

func (c *contentRouter) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	if !strings.HasPrefix(key, "/ipns/") {
		return nil, routing.ErrNotSupported
	}

	name, err := ipns.NameFromRoutingKey([]byte(key))
	if err != nil {
		return nil, err
	}

	record, err := c.client.FindIPNS(ctx, name)
	if err != nil {
		return nil, err
	}

	return ipns.MarshalRecord(record)
}

func (c *contentRouter) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	if !strings.HasPrefix(key, "/ipns/") {
		return nil, routing.ErrNotSupported
	}

	name, err := ipns.NameFromRoutingKey([]byte(key))
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte)

	go func() {
		record, err := c.client.FindIPNS(ctx, name)
		if err != nil {
			close(ch)
			return
		}

		raw, err := ipns.MarshalRecord(record)
		if err != nil {
			close(ch)
			return
		}

		ch <- raw
		close(ch)
	}()

	return ch, nil
}
