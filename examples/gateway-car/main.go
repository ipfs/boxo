package main

import (
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-libipfs/gateway"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
)

func main() {
	carFilePtr := flag.String("c", "", "path to CAR file to back this gateway from")
	portPtr := flag.Int("p", 8080, "port to run this gateway from")
	flag.Parse()

	blockService, root, f, err := newBlockServiceFromCAR(*carFilePtr)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	gateway, err := newBlocksGateway(blockService)
	if err != nil {
		log.Fatal(err)
	}

	handler := newHandler(gateway, *portPtr)

	address := "127.0.0.1:" + strconv.Itoa(*portPtr)
	log.Printf("Listening on http://%s", address)
	log.Printf("Hosting CAR root at http://%s/ipfs/%s", address, root.String())

	if err := http.ListenAndServe(address, handler); err != nil {
		log.Fatal(err)
	}
}

func newBlockServiceFromCAR(filepath string) (blockservice.BlockService, *cid.Cid, io.Closer, error) {
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

	if len(roots) == 0 {
		return nil, nil, nil, errors.New("provided CAR file has no roots")
	}

	blockService := blockservice.New(bs, offline.Exchange(bs))
	return blockService, &roots[0], r, nil
}

func newHandler(gw *blocksGateway, port int) http.Handler {
	headers := map[string][]string{}
	gateway.AddAccessControlHeaders(headers)

	conf := gateway.Config{
		Headers: headers,
	}

	mux := http.NewServeMux()
	gwHandler := gateway.NewHandler(conf, gw)
	mux.Handle("/ipfs/", gwHandler)
	mux.Handle("/ipns/", gwHandler)
	return mux
}
