package client

import (
	"context"
	"net/http"
	"net/url"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Client interface {
	FindProviders(ctx context.Context, cid cid.Cid) ([]peer.AddrInfo, error)
	FindProvidersAsync(ctx context.Context, cid cid.Cid) (<-chan FindProvidersAsyncResult, error)
}

type Option func(*client) error

type client struct {
	client   *http.Client
	endpoint *url.URL
}

func WithHTTPClient(hc *http.Client) Option {
	return func(c *client) error {
		c.client = hc
		return nil
	}
}

func New(endpoint string, opts ...Option) (*client, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	c := &client{endpoint: u, client: http.DefaultClient}
	for _, o := range opts {
		if err := o(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}
