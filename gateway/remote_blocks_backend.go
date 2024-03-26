package gateway

import (
	"net/http"
	"time"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/exchange/offline"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// TODO: make this configurable via BlocksBackendOption
const getBlockTimeout = time.Second * 60

// NewRemoteBlocksBackend creates a new [BlocksBackend] instance backed by one
// or more gateways. These gateways must support RAW block requests and IPNS
// Record requests. See [NewRemoteBlockstore] and [NewRemoteValueStore] for
// more details.
//
// If you want to create a more custom [BlocksBackend] with only remote IPNS
// Record resolution, or only remote block fetching, we recommend using
// [NewBlocksBackend] directly.
func NewRemoteBlocksBackend(gatewayURL []string, opts ...BackendOption) (*BlocksBackend, error) {
	blockStore, err := NewRemoteBlockstore(gatewayURL)
	if err != nil {
		return nil, err
	}

	valueStore, err := NewRemoteValueStore(gatewayURL)
	if err != nil {
		return nil, err
	}

	blockService := blockservice.New(blockStore, offline.Exchange(blockStore))
	return NewBlocksBackend(blockService, append(opts, WithValueStore(valueStore))...)
}

// newRemoteHTTPClient creates a new [http.Client] that is optimized for retrieving
// multiple blocks from a single gateway concurrently.
func newRemoteHTTPClient() *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        1000,
		MaxConnsPerHost:     100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
	}

	return &http.Client{
		Timeout:   getBlockTimeout,
		Transport: otelhttp.NewTransport(transport),
	}
}
