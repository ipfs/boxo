package types

import (
	"github.com/libp2p/go-libp2p/core/peer"
)

const SchemaBitswap = "bitswap"

var _ ProviderResponse = &ReadBitswapProviderRecord{}

// ReadBitswapProviderRecord is a provider result with parameters for bitswap providers
type ReadBitswapProviderRecord struct {
	Protocol string
	Schema   string
	ID       *peer.ID
	Addrs    []Multiaddr
}

func (rbpr *ReadBitswapProviderRecord) GetProtocol() string {
	return rbpr.Protocol
}

func (rbpr *ReadBitswapProviderRecord) GetSchema() string {
	return rbpr.Schema
}

func (*ReadBitswapProviderRecord) IsReadProviderRecord() {}
