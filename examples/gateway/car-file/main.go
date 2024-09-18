package main

import (
	"context"
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
	"github.com/ipfs/go-cid"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	carFilePtr := flag.String("c", "", "path to CAR file to back this gateway from")
	port := flag.Int("p", 8040, "port to run this gateway from")
	flag.Parse()

	// Setups up tracing. This is optional and only required if the implementer
	// wants to be able to enable tracing.
	tp, err := common.SetupTracing(ctx, "CAR Gateway Example")
	if err != nil {
		log.Fatal(err)
	}
	defer (func() { _ = tp.Shutdown(ctx) })()

	// Sets up a block service based on the CAR file.
	blockService, roots, f, err := newBlockServiceFromCAR(*carFilePtr)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Creates the gateway API with the block service.
	backend, err := gateway.NewBlocksBackend(blockService)
	if err != nil {
		log.Fatal(err)
	}

	handler := common.NewHandler(backend)

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
