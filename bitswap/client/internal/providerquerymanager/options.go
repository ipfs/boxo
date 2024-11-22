package providerquerymanager

import (
	"time"
)

const (
	defaultFindProviderTimeout = 10 * time.Second
	defaultMaxConcurrentFinds  = 16
	defaultMaxProvidersPerFind = 10
)

type config struct {
	findProviderTimeout time.Duration
	maxConcurrentFinds  int
	maxProvidersPerFind int
}

type Option func(*config)

func getOpts(opts []Option) config {
	cfg := config{
		findProviderTimeout: defaultFindProviderTimeout,
		maxConcurrentFinds:  defaultMaxConcurrentFinds,
		maxProvidersPerFind: defaultMaxProvidersPerFind,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// WitrFindProviderTimeout configures the maximum amount of time to spend on a
// single find providers attempt. This value can be changed at runtime using
// SetFindProviderTimeout. Use 0 to configure the default.
func WithFindProviderTimeout(to time.Duration) Option {
	return func(c *config) {
		if to == 0 {
			to = defaultFindProviderTimeout
		}
		c.findProviderTimeout = to
	}
}

// WithMaxConcurrentFinds configures the maxmum number of workers that run
// FindProvidersAsync at the same time. Use 0 to configure the default value.
func WithMaxConcurrentFinds(n int) Option {
	return func(c *config) {
		if n == 0 {
			n = defaultMaxConcurrentFinds
		} else if n < 0 {
			panic("bitswap: WithMaxConcurrentFinds given negative value")
		}
		c.maxConcurrentFinds = n
	}
}

// WithMaxProvidersPerFind configures the maximum number of providers that are
// returned from a single fiond providers attempt. Use 0 to configure the
// default value or use a negative value to find all providers within the
// timeout configured by WithFindProviderTimeout.
func WithMaxProvidersPerFind(n int) Option {
	return func(c *config) {
		if n == 0 {
			n = defaultMaxProvidersPerFind
		} else if n < 0 {
			n = 0 // 0 means find all
		}
		c.maxProvidersPerFind = n
	}
}
