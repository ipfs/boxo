package libp2p

import (
	"os"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
)

// Complete list of default options and when to fallback on them.
//
// Please *DON'T* specify default options any other way. Putting this all here
// makes tracking defaults *much* easier.
var defaults = []struct {
	fallback func(cfg *libp2p.Config) bool
	opt      libp2p.Option
}{
	{
		fallback: func(cfg *libp2p.Config) bool { return cfg.Muxers == nil },
		opt:      libp2p.ChainOptions(libp2p.Muxer(yamux.ID, yamuxTransport()), libp2p.Muxer(mplex.ID, mplex.DefaultTransport)),
	},
}

func yamuxTransport() network.Multiplexer {
	tpt := *yamux.DefaultTransport
	tpt.AcceptBacklog = 512
	if os.Getenv("YAMUX_DEBUG") != "" {
		tpt.LogOutput = os.Stderr
	}
	return &tpt
}

// fallbackDefaults applies default options to the libp2p node if and only if no
// other relevant options have been applied. will be appended to the options
// passed into NewHost.
var fallbackDefaults libp2p.Option = func(cfg *libp2p.Config) error {
	for _, def := range defaults {
		if !def.fallback(cfg) {
			continue
		}
		if err := cfg.Apply(def.opt); err != nil {
			return err
		}
	}
	return nil
}

// NewHost creates a new libp2p host with default options found helpful by some existing IPFS implementations.
// These defaults are not guaranteed to be stable over time and any new options provided will replace ones
// of the existing type.
//
// For example, adding a new transport will remove all the existing transports unless they are also added back in the
// passed options.
//
// See the libp2p options for more information
func NewHost(opts ...libp2p.Option) (host.Host, error) {
	return libp2p.New(append(opts, fallbackDefaults)...)
}
