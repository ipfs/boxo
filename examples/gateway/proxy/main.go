package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"

	"github.com/ipfs/go-blockservice"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-libipfs/examples/gateway/common"
	"github.com/ipfs/go-libipfs/gateway"
)

func main() {
	gatewayUrlPtr := flag.String("g", "", "gateway to proxy to")
	portPtr := flag.Int("p", 8080, "port to run this gateway from")
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
	handler := common.NewBlocksHandler(gwAPI, *portPtr)

	// Initialize the public gateways that we will want to have available through
	// Host header rewritting. This step is optional and only required if you're
	// running multiple public gateways and want different settings and support
	// for DNSLink and Subdomain Gateways.
	publicGateways := map[string]*gateway.Specification{
		"localhost": {
			Paths:         []string{"/ipfs", "/ipns"},
			NoDNSLink:     true,
			UseSubdomains: true,
		},
	}
	noDNSLink := true
	handler = gateway.WithHostname(handler, gwAPI, publicGateways, noDNSLink)

	log.Printf("Listening on http://localhost:%d", *portPtr)
	if err := http.ListenAndServe(":"+strconv.Itoa(*portPtr), handler); err != nil {
		log.Fatal(err)
	}
}
