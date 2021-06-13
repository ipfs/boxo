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
	peer "github.com/libp2p/go-libp2p-core/peer"
	p2ptestutil "github.com/libp2p/go-libp2p-netutil"
	tnet "github.com/libp2p/go-libp2p-testing/net"
)

// NewTestInstanceGenerator generates a new InstanceGenerator for the given
// testnet
func NewTestInstanceGenerator(net tn.Network, netOptions []bsnet.NetOpt, bsOptions []bitswap.Option) InstanceGenerator {
	ctx, cancel := context.WithCancel(context.Background())
	return InstanceGenerator{
		net:        net,
		seq:        0,
		ctx:        ctx, // TODO take ctx as param to Next, Instances
		cancel:     cancel,
		bsOptions:  bsOptions,
		netOptions: netOptions,
	}
}

// InstanceGenerator generates new test instances of bitswap+dependencies
type InstanceGenerator struct {
	seq        int
	net        tn.Network
	ctx        context.Context
	cancel     context.CancelFunc
	bsOptions  []bitswap.Option
	netOptions []bsnet.NetOpt
}

// Close closes the clobal context, shutting down all test instances
func (g *InstanceGenerator) Close() error {
	g.cancel()
	return nil // for Closer interface
}

// Next generates a new instance of bitswap + dependencies
func (g *InstanceGenerator) Next() Instance {
	g.seq++
	p, err := p2ptestutil.RandTestBogusIdentity()
	if err != nil {
		panic("FIXME") // TODO change signature
	}
	return NewInstance(g.ctx, g.net, p, g.netOptions, g.bsOptions)
}

// Instances creates N test instances of bitswap + dependencies and connects
// them to each other
func (g *InstanceGenerator) Instances(n int) []Instance {
	var instances []Instance
	for j := 0; j < n; j++ {
		inst := g.Next()
		instances = append(instances, inst)
	}
	ConnectInstances(instances)
	return instances
}

// ConnectInstances connects the given instances to each other
func ConnectInstances(instances []Instance) {
	for i, inst := range instances {
		for j := i + 1; j < len(instances); j++ {
			oinst := instances[j]
			err := inst.Adapter.ConnectTo(context.Background(), oinst.Peer)
			if err != nil {
				panic(err.Error())
			}
		}
	}
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

// NewInstance creates a test bitswap instance.
//
// NB: It's easy make mistakes by providing the same peer ID to two different
// instances. To safeguard, use the InstanceGenerator to generate instances. It's
// just a much better idea.
func NewInstance(ctx context.Context, net tn.Network, p tnet.Identity, netOptions []bsnet.NetOpt, bsOptions []bitswap.Option) Instance {
	bsdelay := delay.Fixed(0)

	adapter := net.Adapter(p, netOptions...)
	dstore := ds_sync.MutexWrap(delayed.New(ds.NewMapDatastore(), bsdelay))

	bstore, err := blockstore.CachedBlockstore(ctx,
		blockstore.NewBlockstore(ds_sync.MutexWrap(dstore)),
		blockstore.DefaultCacheOpts())
	if err != nil {
		panic(err.Error()) // FIXME perhaps change signature and return error.
	}

	bs := bitswap.New(ctx, adapter, bstore, bsOptions...).(*bitswap.Bitswap)

	return Instance{
		Adapter:         adapter,
		Peer:            p.ID(),
		Exchange:        bs,
		blockstore:      bstore,
		blockstoreDelay: bsdelay,
	}
}
