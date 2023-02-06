package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"

	"github.com/ipfs/go-blockservice"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-libipfs/examples/gateway/common"
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
	gateway, err := common.NewBlocksGateway(blockService, routing)
	if err != nil {
		log.Fatal(err)
	}

	handler := common.NewBlocksHandler(gateway, *portPtr)
	address := "127.0.0.1:" + strconv.Itoa(*portPtr)
	log.Printf("Listening on http://%s", address)

	if err := http.ListenAndServe(address, handler); err != nil {
		log.Fatal(err)
	}
}
