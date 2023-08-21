package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestBitswapFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := makeHost(0, 0)
	if err != nil {
		t.Fatal(err)
	}

	client, err := makeHost(0, 0)
	if err != nil {
		t.Fatal(err)
	}

	c, bs, err := startDataServer(ctx, server)
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()

	if expectedCid := cid.MustParse(fileCid); !expectedCid.Equals(c) {
		t.Fatalf("expected CID %s, got %s", expectedCid, c)
	}

	multiaddrs, err := peer.AddrInfoToP2pAddrs(&peer.AddrInfo{
		ID:    server.ID(),
		Addrs: server.Addrs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(multiaddrs) != 1 {
		t.Fatalf("expected a single multiaddr")
	}
	outputBytes, err := runClient(ctx, client, c, multiaddrs[0].String())
	if err != nil {
		t.Fatal(err)
	}
	fileBytes, err := createFile0to100k()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(outputBytes, fileBytes) {
		t.Fatalf("retrieved bytes did not match sent bytes")
	}
}
