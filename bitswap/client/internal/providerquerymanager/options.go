package providerquerymanager

const (
	defaultMaxConcurrentFinds  = 16
	defaultMaxProvidersPerFind = 10
)

type config struct {
	maxConcurrentFinds  int
	maxProvidersPerFind int
}

type Option func(*config)

func getOpts(opts []Option) config {
	cfg := config{
		maxConcurrentFinds:  defaultMaxConcurrentFinds,
		maxProvidersPerFind: defaultMaxProvidersPerFind,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func WithMaxConcurrentFinds(n int) Option {
	return func(c *config) {
		if n == 0 {
			n = defaultMaxConcurrentFinds
		}
		c.maxConcurrentFinds = n
	}
}

func WithMaxProvidersPerFind(n int) Option {
	return func(c *config) {
		if n == 0 {
			n = defaultMaxProvidersPerFind
		}
		c.maxProvidersPerFind = n
	}
}
