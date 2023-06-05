package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"strconv"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/examples/gateway/common"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gatewayUrlPtr := flag.String("g", "", "gateway to proxy to")
	port := flag.Int("p", 8040, "port to run this gateway from")
	flag.Parse()

	// Setups up tracing. This is optional and only required if the implementer
	// wants to be able to enable tracing.
	tp, err := common.SetupTracing(ctx, "CAR Gateway Example")
	if err != nil {
		log.Fatal(err)
	}
	defer (func() { _ = tp.Shutdown(ctx) })()

	// Sets up a blockstore to hold the blocks we request from the gateway
	// Note: in a production environment you would likely want to choose a more efficient datastore implementation
	// as well as one that has a way of pruning storage so as not to hold data in memory indefinitely.
	blockStore := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))

	// Sets up the exchange, which will proxy the block requests to the given gateway.
	e := newProxyExchange(*gatewayUrlPtr, nil)
	blockService := blockservice.New(blockStore, e)

	// Sets up the routing system, which will proxy the IPNS routing requests to the given gateway.
	routing := newProxyRouting(*gatewayUrlPtr, nil)

	// Creates the gateway with the block service and the routing.
	backend, err := gateway.NewBlocksBackend(blockService, gateway.WithValueStore(routing))
	if err != nil {
		log.Fatal(err)
	}

	handler := common.NewHandler(backend)

	log.Printf("Listening on http://localhost:%d", *port)
	log.Printf("Try loading an image: http://localhost:%d/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi", *port)
	log.Printf("Try browsing Wikipedia snapshot: http://localhost:%d/ipfs/bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze", *port)
	log.Printf("Metrics available at http://127.0.0.1:%d/debug/metrics/prometheus", *port)
	if err := http.ListenAndServe(":"+strconv.Itoa(*port), handler); err != nil {
		log.Fatal(err)
	}
}
