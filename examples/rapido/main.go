package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/ipsl/unixfs"
	"github.com/ipfs/go-libipfs/rapide"
	"github.com/ipfs/go-libipfs/rapide/gateway"
	"github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
)

type stringFlagSlice []string

func (sfs *stringFlagSlice) String() string {
	return strings.Join(*sfs, ",")
}

func (sfs *stringFlagSlice) Set(s string) error {
	*sfs = append(*sfs, s)
	return nil
}

func main() {
	err := mainRet()
	if err != nil {
		os.Stderr.WriteString(err.Error())
		os.Exit(1)
	}
	os.Exit(0)
}

func mainRet() error {
	// Argument Parsing
	var gateways stringFlagSlice
	flag.Var(&gateways, "gw", `Set once to add a gateway, setable multiple times to use multiple gateways. Format expected is "-gw https://ipfs.io/ipfs/" for example.`)
	flag.Parse()

	cidStrs := flag.Args()
	if len(cidStrs) != 1 {
		return fmt.Errorf("expected one CID as positional argument; got %d", len(cidStrs))
	}

	root, err := cid.Decode(cidStrs[0])
	if err != nil {
		return fmt.Errorf("decoding CID: %w", err)
	}

	// Setup header for the output car
	err = car.WriteHeader(&car.CarHeader{
		Roots:   []cid.Cid{root},
		Version: 1,
	}, os.Stdout)
	if err != nil {
		return fmt.Errorf("writing car header: %w", err)
	}

	// configure rapide
	downloaders := make([]rapide.ServerDrivenDownloader, len(gateways)) // create a slice holding our multiple gateways
	for i, g := range gateways {
		downloaders[i] = gateway.Gateway{PathName: g} // create a gateway protocol implementation
	}
	client := rapide.Client{ServerDrivenDownloaders: downloaders}

	// do request and iterate over the resulting blocks, rapide.(*Client).Get returns a channel
	for maybeBlock := range client.Get(context.Background(), root, unixfs.Everything()) {
		block, err := maybeBlock.Get() // block or error ?
		if err != nil {
			return fmt.Errorf("downloading: %w", err)
		}
		err = util.LdWrite(os.Stdout, block.Cid().Bytes(), block.RawData()) // write to the output car
		if err != nil {
			return fmt.Errorf("writing to output car: %w", err)
		}
	}

	return nil
}
