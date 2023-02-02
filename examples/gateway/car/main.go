package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-libipfs/examples/gateway/common"
	"github.com/ipfs/go-libipfs/gateway"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
)

func main() {
	carFilePtr := flag.String("c", "", "path to CAR file to back this gateway from")
	portPtr := flag.Int("p", 8080, "port to run this gateway from")
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
	for _, cid := range roots {
		log.Printf("Hosting CAR root at http://localhost:%d/ipfs/%s", *portPtr, cid.String())
	}

	if err := http.ListenAndServe(":"+strconv.Itoa(*portPtr), handler); err != nil {
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
