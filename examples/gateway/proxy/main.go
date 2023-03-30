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
	gwAPI, err := gateway.NewBlocksGateway(blockService, gateway.WithValueStore(routing))
	if err != nil {
		log.Fatal(err)
	}

	handler := common.NewHandler(gwAPI)

	log.Printf("Listening on http://localhost:%d", *port)
	log.Printf("Try loading an image: http://localhost:%d/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi", *port)
	log.Printf("Try browsing Wikipedia snapshot: http://localhost:%d/ipfs/bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze", *port)
	log.Printf("Metrics available at http://127.0.0.1:%d/debug/metrics/prometheus", *port)
	if err := http.ListenAndServe(":"+strconv.Itoa(*port), handler); err != nil {
		log.Fatal(err)
	}
}
