package mockrouting

import (
	"context"
	"time"

	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-testing/net"

	ma "github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("mockrouter")

type client struct {
	vs     routing.ValueStore
	server server
	peer   tnet.Identity
}

// FIXME(brian): is this method meant to simulate putting a value into the network?
func (c *client) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) error {
	log.Debugf("PutValue: %s", key)
	return c.vs.PutValue(ctx, key, val, opts...)
}

// FIXME(brian): is this method meant to simulate getting a value from the network?
func (c *client) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	log.Debugf("GetValue: %s", key)
	return c.vs.GetValue(ctx, key, opts...)
}

func (c *client) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	log.Debugf("SearchValue: %s", key)
	return c.vs.SearchValue(ctx, key, opts...)
}

func (c *client) FindProviders(ctx context.Context, key cid.Cid) ([]peer.AddrInfo, error) {
	return c.server.Providers(key), nil
}

func (c *client) FindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	log.Debugf("FindPeer: %s", pid)
	return peer.AddrInfo{}, nil
}

func (c *client) FindProvidersAsync(ctx context.Context, k cid.Cid, max int) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	go func() {
		defer close(out)
		for i, p := range c.server.Providers(k) {
			if max <= i {
				return
			}
			select {
			case out <- p:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// Provide returns once the message is on the network. Value is not necessarily
// visible yet.
func (c *client) Provide(_ context.Context, key cid.Cid, brd bool) error {
	if !brd {
		return nil
	}
	info := peer.AddrInfo{
		ID:    c.peer.ID(),
		Addrs: []ma.Multiaddr{c.peer.Address()},
	}
	return c.server.Announce(info, key)
}

func (c *client) Ping(ctx context.Context, p peer.ID) (time.Duration, error) {
	return 0, nil
}

func (c *client) Bootstrap(context.Context) error {
	return nil
}

var _ routing.Routing = &client{}
