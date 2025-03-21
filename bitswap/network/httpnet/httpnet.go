// Package httpnet implements an Exchange network that sends and receives
// Exchange messages from peers' HTTP endpoints.
package httpnet

import (
	"time"

	"github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/swap/httpswap"
	"github.com/libp2p/go-libp2p/core/host"
)

// Deprecated: use github.com/ipfs/boxo/swap/httpswap
// Option allows to configure the Network.
type Option httpswap.Option

// Deprecated: use github.com/ipfs/boxo/swap/httpswap
//
// WithUserAgent sets the user agent when making requests.
func WithUserAgent(agent string) Option {
	return Option(httpswap.WithUserAgent(agent))
}

// Deprecated: use github.com/ipfs/boxo/swap/httpswap
//
// WithMaxBlockSize sets the maximum size of an HTTP response (block).
func WithMaxBlockSize(size int64) Option {
	return Option(httpswap.WithMaxBlockSize(size))
}

// Deprecated: use github.com/ipfs/boxo/swap/httpswap
//
// WithDialTimeout sets the maximum time to wait for a connection to be set up.
func WithDialTimeout(t time.Duration) Option {
	return Option(httpswap.WithDialTimeout(t))
}

// Deprecated: use github.com/ipfs/boxo/swap/httpswap
//
// WithIdleConnTimeout sets how long to keep connections alive before closing
// them when no requests happen.
func WithIdleConnTimeout(t time.Duration) Option {
	return Option(httpswap.WithIdleConnTimeout(t))
}

// Deprecated: use github.com/ipfs/boxo/swap/httpswap
//
// WithResponseHeaderTimeout sets how long to wait for a response to start
// arriving. It is the time given to the provider to find and start sending
// the block. It does not affect the time it takes to download the request body.
func WithResponseHeaderTimeout(t time.Duration) Option {
	return Option(httpswap.WithResponseHeaderTimeout(t))
}

// Deprecated: use github.com/ipfs/boxo/swap/httpswap
//
// WithMaxIdleConns sets how many keep-alive connections we can have where no
// requests are happening.
func WithMaxIdleConns(n int) Option {
	return Option(httpswap.WithMaxIdleConns(n))
}

// Deprecated: use github.com/ipfs/boxo/swap/httpswap
//
// WithInsecureSkipVerify allows making HTTPS connections to test servers.
// Use for testing.
func WithInsecureSkipVerify(b bool) Option {
	return Option(httpswap.WithInsecureSkipVerify(b))
}

// Deprecated: use github.com/ipfs/boxo/swap/httpswap
//
// WithAllowlist sets the hostnames that we are allowed to connect to via
// HTTP. Additionally, http response status metrics are tagged for each of
// these hosts.
func WithAllowlist(hosts []string) Option {
	return Option(httpswap.WithAllowlist(hosts))
}

// Deprecated: use github.com/ipfs/boxo/swap/httpswap
func WithDenylist(hosts []string) Option {
	return Option(httpswap.WithDenylist(hosts))
}

// Deprecated: use github.com/ipfs/boxo/swap/httpswap
//
// WithMaxHTTPAddressesPerPeer limits how many http addresses we attempt to
// connect to per peer.
func WithMaxHTTPAddressesPerPeer(max int) Option {
	return Option(httpswap.WithMaxHTTPAddressesPerPeer(max))
}

// Deprecated: use github.com/ipfs/boxo/swap/httpswap
//
// WithHTTPWorkers controls how many HTTP requests can be done concurrently.
func WithHTTPWorkers(n int) Option {
	return Option(httpswap.WithHTTPWorkers(n))
}

func convertOpts(in []Option) (out []httpswap.Option) {
	if in == nil {
		return
	}

	out = make([]httpswap.Option, len(in))
	for i, o := range in {
		out[i] = httpswap.Option(o)
	}
	return
}

// Deprecated: use github.com/ipfs/boxo/swap/httpswap
type Network httpswap.Network

// Deprecated: use github.com/ipfs/boxo/swap/httpswap
//
// New returns a BitSwapNetwork supported by underlying IPFS host.
func New(host host.Host, opts ...Option) network.BitSwapNetwork {
	return httpswap.New(host, convertOpts(opts)...)
}
