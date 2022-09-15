package test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-delegated-routing/client"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

func TestProvideRoundtrip(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}

	c1, s1 := createClientAndServer(t, testDelegatedRoutingService{}, nil, nil)
	defer s1.Close()

	testMH, _ := multihash.Encode([]byte("test"), multihash.IDENTITY)
	testCid := cid.NewCidV1(cid.Raw, testMH)

	if _, err = c1.Provide(context.Background(), []cid.Cid{testCid}, time.Hour); err == nil {
		t.Fatal("should get sync error on unsigned provide request.")
	}

	ma, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/4001")
	if err != nil {
		t.Fatal(err)
	}

	c, s := createClientAndServer(t, testDelegatedRoutingService{}, &client.Provider{
		Peer: peer.AddrInfo{
			ID:    pID,
			Addrs: []multiaddr.Multiaddr{ma},
		},
		ProviderProto: []client.TransferProtocol{{Codec: multicodec.TransportBitswap}},
	}, priv)
	defer s.Close()

	rc, err := c.Provide(context.Background(), []cid.Cid{testCid}, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	if rc != time.Hour {
		t.Fatal("should have gotten back the the fixed server ttl")
	}
}
