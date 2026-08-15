package client_test

import (
	"context"

	"github.com/ipfs/boxo/bitswap/client"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	"github.com/ipfs/boxo/blockstore"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
)

// ExampleNew_broadcastControl constructs a Bitswap client with broadcast
// reduction enabled, so that broadcasts go only to peers that are likely to
// respond, plus up to 5 random peers per broadcast. See
// https://github.com/ipfs/boxo/blob/main/docs/broadcastcontrol.md for how the
// options interact.
func ExampleNew_broadcastControl() {
	ctx := context.Background()

	host, err := libp2p.New()
	if err != nil {
		panic(err)
	}
	network := bsnet.NewFromIpfsHost(host)
	bstore := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))

	// The provider finder is nil: content discovery relies on bitswap alone.
	bswap := client.New(ctx, network, nil, bstore,
		client.BroadcastControlEnable(true),
		client.BroadcastControlMaxRandomPeers(5),
	)
	network.Start(bswap)

	defer network.Stop()
	defer bswap.Close()
}
