package client

import (
	"time"

	"github.com/ipfs/boxo/bitswap/internal/defaults"
	"github.com/ipfs/boxo/bitswap/tracer"
	delay "github.com/ipfs/go-ipfs-delay"
)

type clientConfig struct {
	blockReceivedNotifier      BlockReceivedNotifier
	provSearchDelay            time.Duration
	rebroadcastDelay           delay.D
	simulateDontHavesOnTimeout bool
	skipDuplicatedBlocksStats  bool
	tracer                     tracer.Tracer

	// ProviderQueryManager options.
	pqmMaxConcurrentFinds  int
	pqmMaxProvidersPerFind int
}

// Option defines the functional option type that can be used to configure
// bitswap instances
type Option func(*clientConfig)

func getOpts(opts []Option) clientConfig {
	cfg := clientConfig{
		provSearchDelay:            defaults.ProvSearchDelay,
		rebroadcastDelay:           delay.Fixed(defaults.RebroadcastDelay),
		simulateDontHavesOnTimeout: true,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// ProviderSearchDelay sets the initial dely before triggering a provider
// search to find more peers and broadcast the want list. It also partially
// controls re-broadcasts delay when the session idles (does not receive any
// blocks), but these have back-off logic to increase the interval. See
// [defaults.ProvSearchDelay] for the default.
func ProviderSearchDelay(newProvSearchDelay time.Duration) Option {
	return func(c *clientConfig) {
		c.provSearchDelay = newProvSearchDelay
	}
}

// RebroadcastDelay sets a custom delay for periodic search of a random want.
// When the value ellapses, a random CID from the wantlist is chosen and the
// client attempts to find more peers for it and sends them the single want.
// [defaults.RebroadcastDelay] for the default.
func RebroadcastDelay(newRebroadcastDelay delay.D) Option {
	return func(c *clientConfig) {
		c.rebroadcastDelay = newRebroadcastDelay
	}
}

func SetSimulateDontHavesOnTimeout(send bool) Option {
	return func(c *clientConfig) {
		c.simulateDontHavesOnTimeout = send
	}
}

// Configures the Client to use given tracer.
// This provides methods to access all messages sent and received by the Client.
// This interface can be used to implement various statistics (this is original intent).
func WithTracer(tap tracer.Tracer) Option {
	return func(c *clientConfig) {
		c.tracer = tap
	}
}

func WithBlockReceivedNotifier(brn BlockReceivedNotifier) Option {
	return func(c *clientConfig) {
		c.blockReceivedNotifier = brn
	}
}

// WithoutDuplicatedBlockStats disable collecting counts of duplicated blocks
// received. This counter requires triggering a blockstore.Has() call for
// every block received by launching goroutines in parallel. In the worst case
// (no caching/blooms etc), this is an expensive call for the datastore to
// answer. In a normal case (caching), this has the power of evicting a
// different block from intermediary caches. In the best case, it doesn't
// affect performance. Use if this stat is not relevant.
func WithoutDuplicatedBlockStats() Option {
	return func(c *clientConfig) {
		c.skipDuplicatedBlocksStats = true
	}
}

func WithPQMMaxConcurrentFinds(n int) Option {
	return func(c *clientConfig) {
		c.pqmMaxConcurrentFinds = n
	}
}

func WithPQMMaxProvidersPerFind(n int) Option {
	return func(c *clientConfig) {
		c.pqmMaxProvidersPerFind = n
	}
}
