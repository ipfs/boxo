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
	handler = gateway.WithHostname(handler, gwAPI, publicGateways, noDNSLink)

	log.Printf("Listening on http://localhost:%d", *port)
	if err := http.ListenAndServe(":"+strconv.Itoa(*port), handler); err != nil {
		log.Fatal(err)
	}
}
