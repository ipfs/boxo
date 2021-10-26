package test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-delegated-routing/client"
	"github.com/ipfs/go-delegated-routing/server"
	"github.com/libp2p/go-libp2p-core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

func TestClientServer(t *testing.T) {
	// start a server
	s := httptest.NewServer(server.FindProvidersAsyncHandler(testFindProvidersAsyncFunc))
	defer s.Close()
	// start a client
	c, err := client.New(s.URL, client.WithHTTPClient(s.Client()))
	if err != nil {
		t.Fatal(err)
	}
	// verify result
	h, err := multihash.Sum([]byte("TEST"), multihash.SHA3, 4)
	if err != nil {
		t.Fatal(err)
	}
	infos, err := c.FindProviders(context.Background(), cid.NewCidV1(cid.Libp2pKey, h))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(infos)
}

func testFindProvidersAsyncFunc(key cid.Cid, ch chan<- client.FindProvidersAsyncResult) error {
	ma := multiaddr.StringCast("/ip4/7.7.7.7/tcp/4242/p2p/QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N")
	ai, err := peer.AddrInfoFromP2pAddr(ma)
	if err != nil {
		println(err.Error())
		return fmt.Errorf("address info creation (%v)", err)
	}
	go func() {
		ch <- client.FindProvidersAsyncResult{AddrInfo: []peer.AddrInfo{*ai}}
		close(ch)
	}()
	return nil
}
