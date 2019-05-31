// Package nilrouting implements a routing client that does nothing.
package nilrouting

import (
	"context"
	"errors"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"

	record "github.com/libp2p/go-libp2p-record"
)

type nilclient struct {
}

func (c *nilclient) PutValue(_ context.Context, _ string, _ []byte, _ ...routing.Option) error {
	return nil
}

func (c *nilclient) GetValue(_ context.Context, _ string, _ ...routing.Option) ([]byte, error) {
	return nil, errors.New("tried GetValue from nil routing")
}

func (c *nilclient) SearchValue(_ context.Context, _ string, _ ...routing.Option) (<-chan []byte, error) {
	return nil, errors.New("tried SearchValue from nil routing")
}

func (c *nilclient) FindPeer(_ context.Context, _ peer.ID) (peer.AddrInfo, error) {
	return peer.AddrInfo{}, nil
}

func (c *nilclient) FindProvidersAsync(_ context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
	out := make(chan peer.AddrInfo)
	defer close(out)
	return out
}

func (c *nilclient) Provide(_ context.Context, _ cid.Cid, _ bool) error {
	return nil
}

func (c *nilclient) Bootstrap(_ context.Context) error {
	return nil
}

// ConstructNilRouting creates an Routing client which does nothing.
func ConstructNilRouting(_ context.Context, _ host.Host, _ ds.Batching, _ record.Validator) (routing.Routing, error) {
	return &nilclient{}, nil
}

//  ensure nilclient satisfies interface
var _ routing.Routing = &nilclient{}
