package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"strconv"

	"github.com/ipfs/boxo/examples/gateway/common"
	"github.com/ipfs/boxo/gateway"
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

	// Creates the gateway with the remote block store backend.
	backend, err := gateway.NewRemoteBlocksBackend([]string{*gatewayUrlPtr}, nil)
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
