package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/examples/gateway/common"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/gateway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	gatewayUrlPtr := flag.String("g", "", "gateway to proxy to")
	port := flag.Int("p", 8040, "port to run this gateway from")
	flag.Parse()

	// Sets up the block store, which will proxy the block requests to the given gateway.
	blockStore := newProxyStore(*gatewayUrlPtr, nil)
	blockService := blockservice.New(blockStore, offline.Exchange(blockStore))

	// Sets up the routing system, which will proxy the IPNS routing requests to the given gateway.
	routing := newProxyRouting(*gatewayUrlPtr, nil)

	// Creates the gateway with the block service and the routing.
	gwAPI, err := common.NewBlocksGateway(blockService, routing)
	if err != nil {
		log.Fatal(err)
	}
	handler := common.NewBlocksHandler(gwAPI, *port)

	// Initialize the public gateways that we will want to have available through
	// Host header rewritting. This step is optional and only required if you're
	// running multiple public gateways and want different settings and support
	// for DNSLink and Subdomain Gateways.
	noDNSLink := false
	publicGateways := map[string]*gateway.Specification{
		// Support public requests with Host: CID.ipfs.example.net and ID.ipns.example.net
		"example.net": {
			Paths:         []string{"/ipfs", "/ipns"},
			NoDNSLink:     noDNSLink,
			UseSubdomains: true,
		},
		// Support local requests
		"localhost": {
			Paths:         []string{"/ipfs", "/ipns"},
			NoDNSLink:     noDNSLink,
			UseSubdomains: true,
		},
	}

	// Creates a mux to serve the prometheus metrics alongside the gateway. This
	// step is optional and only required if you need or want to access the metrics.
	// You may also decide to expose the metrics on a different path, or port.
	mux := http.NewServeMux()
	mux.Handle("/debug/metrics/prometheus", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}))
	mux.Handle("/", handler)

	// Then wrap the mux with the hostname handler. Please note that the metrics
	// will not be available under the previously defined publicGateways.
	// You will be able to access the metrics via 127.0.0.1 but not localhost
	// or example.net. If you want to expose the metrics on such gateways,
	// you will have to add the path "/debug" to the variable Paths.
	handler = gateway.WithHostname(mux, gwAPI, publicGateways, noDNSLink)

	log.Printf("Listening on http://localhost:%d", *port)
	log.Printf("Try loading an image: http://localhost:%d/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi", *port)
	log.Printf("Try browsing Wikipedia snapshot: http://localhost:%d/ipfs/bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze", *port)
	log.Printf("Metrics available at http://127.0.0.1:%d/debug/metrics/prometheus", *port)
	if err := http.ListenAndServe(":"+strconv.Itoa(*port), handler); err != nil {
		log.Fatal(err)
	}
}
