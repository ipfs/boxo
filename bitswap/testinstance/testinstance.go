package testsession

import (
	"context"
	"time"

	bitswap "github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	tn "github.com/ipfs/go-bitswap/testnet"
	ds "github.com/ipfs/go-datastore"
	delayed "github.com/ipfs/go-datastore/delayed"
	ds_sync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	delay "github.com/ipfs/go-ipfs-delay"
	p2ptestutil "github.com/libp2p/go-libp2p-netutil"
	peer "github.com/libp2p/go-libp2p-peer"
	testutil "github.com/libp2p/go-testutil"
)

// NewTestSessionGenerator generates a new SessionGenerator for the given
// testnet
func NewTestSessionGenerator(
	net tn.Network) SessionGenerator {
	ctx, cancel := context.WithCancel(context.Background())
	return SessionGenerator{
		net:    net,
		seq:    0,
		ctx:    ctx, // TODO take ctx as param to Next, Instances
		cancel: cancel,
	}
}

// SessionGenerator generates new test instances of bitswap+dependencies
type SessionGenerator struct {
	seq    int
	net    tn.Network
	ctx    context.Context
	cancel context.CancelFunc
}

// Close closes the clobal context, shutting down all test instances
func (g *SessionGenerator) Close() error {
	g.cancel()
	return nil // for Closer interface
}

// Next generates a new instance of bitswap + dependencies
func (g *SessionGenerator) Next() Instance {
	g.seq++
	p, err := p2ptestutil.RandTestBogusIdentity()
	if err != nil {
		panic("FIXME") // TODO change signature
	}
	return MkSession(g.ctx, g.net, p)
}

// Instances creates N test instances of bitswap + dependencies
func (g *SessionGenerator) Instances(n int) []Instance {
	var instances []Instance
	for j := 0; j < n; j++ {
		inst := g.Next()
		instances = append(instances, inst)
	}
	for i, inst := range instances {
		for j := i + 1; j < len(instances); j++ {
			oinst := instances[j]
			inst.Adapter.ConnectTo(context.Background(), oinst.Peer)
		}
	}
	return instances
}

// Instance is a test instance of bitswap + dependencies for integration testing
type Instance struct {
	Peer            peer.ID
	Exchange        *bitswap.Bitswap
	blockstore      blockstore.Blockstore
	Adapter         bsnet.BitSwapNetwork
	blockstoreDelay delay.D
}

// Blockstore returns the block store for this test instance
func (i *Instance) Blockstore() blockstore.Blockstore {
	return i.blockstore
}

// SetBlockstoreLatency customizes the artificial delay on receiving blocks
// from a blockstore test instance.
func (i *Instance) SetBlockstoreLatency(t time.Duration) time.Duration {
	return i.blockstoreDelay.Set(t)
}

// MkSession creates a test bitswap instance.
//
// NB: It's easy make mistakes by providing the same peer ID to two different
// sessions. To safeguard, use the SessionGenerator to generate sessions. It's
// just a much better idea.
func MkSession(ctx context.Context, net tn.Network, p testutil.Identity) Instance {
	bsdelay := delay.Fixed(0)

	adapter := net.Adapter(p)
	dstore := ds_sync.MutexWrap(delayed.New(ds.NewMapDatastore(), bsdelay))

	bstore, err := blockstore.CachedBlockstore(ctx,
		blockstore.NewBlockstore(ds_sync.MutexWrap(dstore)),
		blockstore.DefaultCacheOpts())
	if err != nil {
		panic(err.Error()) // FIXME perhaps change signature and return error.
	}

	bs := bitswap.New(ctx, adapter, bstore).(*bitswap.Bitswap)

	return Instance{
		Adapter:         adapter,
		Peer:            p.ID(),
		Exchange:        bs,
		blockstore:      bstore,
		blockstoreDelay: bsdelay,
	}
}
