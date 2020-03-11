package bitswap

import (
	"context"

	bsnet "github.com/ipfs/go-bitswap/network"

	ds "github.com/ipfs/go-datastore"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"

	"github.com/libp2p/go-libp2p-core/peer"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	mockpeernet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

type peernet struct {
	mockpeernet.Mocknet
	routingserver mockrouting.Server
}

// StreamNet is a testnet that uses libp2p's MockNet
func StreamNet(ctx context.Context, net mockpeernet.Mocknet, rs mockrouting.Server) (Network, error) {
	return &peernet{net, rs}, nil
}

func (pn *peernet) Adapter(p tnet.Identity, opts ...bsnet.NetOpt) bsnet.BitSwapNetwork {
	client, err := pn.Mocknet.AddPeer(p.PrivateKey(), p.Address())
	if err != nil {
		panic(err.Error())
	}
	routing := pn.routingserver.ClientWithDatastore(context.TODO(), p, ds.NewMapDatastore())
	return bsnet.NewFromIpfsHost(client, routing, opts...)
}

func (pn *peernet) HasPeer(p peer.ID) bool {
	for _, member := range pn.Mocknet.Peers() {
		if p == member {
			return true
		}
	}
	return false
}

var _ Network = (*peernet)(nil)
