// Package zikade provides routing using the Zikade IPFS DHT implementation.
package zikade

import (
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht/v2"
	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/plprobelab/go-kademlia/routing/triert"
	"go.uber.org/zap/exp/zapslog"
	"golang.org/x/exp/slog"
)

var logger = logging.Logger("routing/dht/zikade")

type config struct {
	dht *dht.Config
	rt  *triert.Config[kadt.Key, kadt.PeerID]
}

// NewRouter creates a new router using the Zikade IPFS DHT implementation.
func NewRouter(h host.Host, opts ...Option) (routing.Routing, error) {
	cfg := &config{
		dht: dht.DefaultConfig(),
		rt:  triert.DefaultConfig[kadt.Key, kadt.PeerID](),
	}
	cfg.dht.ProtocolID = dht.ProtocolIPFS
	cfg.dht.Logger = slog.New(zapslog.NewHandler(logger.Desugar().Core()))

	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, fmt.Errorf("option: %w", err)
		}
	}

	nid := kadt.PeerID(h.ID())
	rt, err := triert.New[kadt.Key, kadt.PeerID](nid, cfg.rt)
	if err != nil {
		return nil, fmt.Errorf("new trie routing table: %w", err)
	}
	cfg.dht.RoutingTable = rt

	d, err := dht.New(h, cfg.dht)

	return d, err
}
