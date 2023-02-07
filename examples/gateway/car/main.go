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
	noDNSLink := true // If you set DNSLink to point at the CID from CAR, you can load it!
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
