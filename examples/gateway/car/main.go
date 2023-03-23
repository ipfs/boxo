package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/examples/gateway/common"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/gateway"
	carblockstore "github.com/ipfs/boxo/ipld/car/v2/blockstore"
	"github.com/ipfs/go-cid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	carFilePtr := flag.String("c", "", "path to CAR file to back this gateway from")
	port := flag.Int("p", 8040, "port to run this gateway from")
	flag.Parse()

	blockService, roots, f, err := newBlockServiceFromCAR(*carFilePtr)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	gwAPI, err := common.NewBlocksGateway(blockService, nil)
	if err != nil {
		log.Fatal(err)
	}
	handler := common.NewBlocksHandler(gwAPI, *port)

	// Initialize the public gateways that we will want to have available through
	// Host header rewritting. This step is optional and only required if you're
	// running multiple public gateways and want different settings and support
	// for DNSLink and Subdomain Gateways.
	noDNSLink := false // If you set DNSLink to point at the CID from CAR, you can load it!
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
	log.Printf("Metrics available at http://127.0.0.1:%d/debug/metrics/prometheus", *port)
	for _, cid := range roots {
		log.Printf("Hosting CAR root at http://localhost:%d/ipfs/%s", *port, cid.String())
	}

	if err := http.ListenAndServe(":"+strconv.Itoa(*port), handler); err != nil {
		log.Fatal(err)
	}
}

func newBlockServiceFromCAR(filepath string) (blockservice.BlockService, []cid.Cid, io.Closer, error) {
	r, err := os.Open(filepath)
	if err != nil {
		return nil, nil, nil, err
	}

	bs, err := carblockstore.NewReadOnly(r, nil)
	if err != nil {
		_ = r.Close()
		return nil, nil, nil, err
	}

	roots, err := bs.Roots()
	if err != nil {
		return nil, nil, nil, err
	}

	blockService := blockservice.New(bs, offline.Exchange(bs))
	return blockService, roots, r, nil
}
