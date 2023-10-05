// Package zikade provides routing using the Zikade IPFS DHT implementation.
package zikade

import (
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/plprobelab/go-libdht/kad/triert"
	"github.com/plprobelab/zikade"
	"github.com/plprobelab/zikade/kadt"
	"github.com/plprobelab/zikade/tele"
	oprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap/exp/zapslog"
	"golang.org/x/exp/slog"
)

var logger = logging.Logger("routing/dht/zikade")

type config struct {
	dht *zikade.Config
	rt  *triert.Config[kadt.Key, kadt.PeerID]
}

// NewRouter creates a new router using the Zikade IPFS DHT implementation.
func NewRouter(h host.Host, opts ...Option) (*zikade.DHT, error) {
	cfg := &config{
		dht: zikade.DefaultConfig(),
		rt:  triert.DefaultConfig[kadt.Key, kadt.PeerID](),
	}
	cfg.dht.ProtocolID = zikade.ProtocolIPFS
	cfg.dht.Logger = slog.New(zapslog.NewHandler(logger.Desugar().Core()))

	exporter, err := oprom.New(oprom.WithNamespace("zikade"))
	if err != nil {
		return nil, fmt.Errorf("prometheus exporter: %w", err)
	}
	cfg.dht.MeterProvider = sdkmetric.NewMeterProvider(append(tele.MeterProviderOpts, sdkmetric.WithReader(exporter))...)

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

	d, err := zikade.New(h, cfg.dht)

	return d, err
}
