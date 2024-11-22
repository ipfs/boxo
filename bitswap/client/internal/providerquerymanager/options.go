package providerquerymanager

const (
	defaultMaxProvidersPerFind  = 10
	defaultMaxInProcessRequests = 6
)

type config struct {
	maxProvidersPerFind  int
	maxInProcessRequests int
}

type Option func(*config)

func getOpts(opts []Option) config {
	cfg := config{
		maxProvidersPerFind:  defaultMaxProvidersPerFind,
		maxInProcessRequests: defaultMaxInProcessRequests,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func WithMaxProvidersPerFind(n int) Option {
	return func(c *config) {
		if n == 0 {
			n = defaultMaxProvidersPerFind
		}
		c.maxProvidersPerFind = n
	}
}

func WithMaxInProcessRequests(n int) Option {
	return func(c *config) {
		if n == 0 {
			n = defaultMaxInProcessRequests
		}
		c.maxInProcessRequests = n
	}
}
