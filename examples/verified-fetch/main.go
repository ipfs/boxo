package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/ipfs/boxo/path"
)

func main() {
	flag.Usage = func() {
		fmt.Println("Usage: verified-fetch [flags] <path>")
		flag.PrintDefaults()
	}

	gatewayUrlPtr := flag.String("g", "https://trustless-gateway.link", "trustless gateway to download the CAR file from")
	userAgentPtr := flag.String("u", "", "user agent to use during the HTTP requests")
	outputPtr := flag.String("o", "out", "output path to store the fetched path")
	limitPtr := flag.Int64("l", 0, "file size limit for the gateway download")
	flag.Parse()

	ipfsPath := flag.Arg(0)
	if len(ipfsPath) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	if err := run(*gatewayUrlPtr, ipfsPath, *outputPtr, *userAgentPtr, *limitPtr); err != nil {
		log.Fatal(err)
	}
}

func run(gatewayURL, ipfsPath, output, userAgent string, limit int64) error {
	p, err := path.NewPath(ipfsPath)
	if err != nil {
		return err
	}

	options := []fetcherOption{
		withUserAgent(userAgent),
		withLimit(limit),
	}

	f, err := newFetcher(gatewayURL, options...)
	if err != nil {
		return err
	}

	return f.fetch(context.Background(), p, output)
}
