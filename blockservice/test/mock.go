package bstest

import (
	testinstance "github.com/ipfs/go-bitswap/testinstance"
	tn "github.com/ipfs/go-bitswap/testnet"
	"github.com/ipfs/go-blockservice"
	delay "github.com/ipfs/go-ipfs-delay"
	mockrouting "github.com/ipfs/go-ipfs-routing/mock"
)

// Mocks returns |n| connected mock Blockservices
func Mocks(n int) []blockservice.BlockService {
	net := tn.VirtualNetwork(mockrouting.NewServer(), delay.Fixed(0))
	sg := testinstance.NewTestInstanceGenerator(net, nil, nil)

	instances := sg.Instances(n)

	var servs []blockservice.BlockService
	for _, i := range instances {
		servs = append(servs, blockservice.New(i.Blockstore(), i.Exchange))
	}
	return servs
}
