package bitswap

import (
	"context"

	iface "github.com/ipfs/boxo/bitswap/network"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/peer"
	mockpeernet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

type peernet struct {
	mockpeernet.Mocknet
}

// StreamNet is a testnet that uses libp2p's MockNet
func StreamNet(ctx context.Context, net mockpeernet.Mocknet) (Network, error) {
	return &peernet{net}, nil
}

func (pn *peernet) Adapter(p tnet.Identity, opts ...bsnet.NetOpt) iface.BitSwapNetwork {
	client, err := pn.Mocknet.AddPeer(p.PrivateKey(), p.Address())
	if err != nil {
		panic(err.Error())
	}

	return bsnet.NewFromIpfsHost(client, opts...)
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
