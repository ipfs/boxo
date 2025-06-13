package testsession

import (
	"context"
	"time"

	"github.com/ipfs/boxo/bitswap"
	iface "github.com/ipfs/boxo/bitswap/network"
	bsnet "github.com/ipfs/boxo/bitswap/network/bsnet"
	tn "github.com/ipfs/boxo/bitswap/testnet"
	blockstore "github.com/ipfs/boxo/blockstore"
	mockrouting "github.com/ipfs/boxo/routing/mock"
	ds "github.com/ipfs/go-datastore"
	delayed "github.com/ipfs/go-datastore/delayed"
	ds_sync "github.com/ipfs/go-datastore/sync"
	delay "github.com/ipfs/go-ipfs-delay"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	p2ptestutil "github.com/libp2p/go-libp2p-testing/netutil"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

// NewTestInstanceGenerator generates a new InstanceGenerator for the given
// testnet
func NewTestInstanceGenerator(net tn.Network, routing mockrouting.Server, netOptions []bsnet.NetOpt, bsOptions []bitswap.Option) InstanceGenerator {
	ctx, cancel := context.WithCancel(context.Background())
	return InstanceGenerator{
		net:        net,
		seq:        0,
		ctx:        ctx, // TODO take ctx as param to Next, Instances
		cancel:     cancel,
		bsOptions:  bsOptions,
		netOptions: netOptions,
		routing:    routing,
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
	routing    mockrouting.Server
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
	return NewInstance(g.ctx, g.net, g.routing.Client(p), p, g.netOptions, g.bsOptions)
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
			err := inst.Adapter.Connect(context.Background(), peer.AddrInfo{ID: oinst.Identity.ID()})
			if err != nil {
				panic(err.Error())
			}
		}
	}
}

// Instance is a test instance of bitswap + dependencies for integration testing
type Instance struct {
	Identity        tnet.Identity
	Datastore       ds.Batching
	Exchange        *bitswap.Bitswap
	Blockstore      blockstore.Blockstore
	Adapter         iface.BitSwapNetwork
	Routing         routing.Routing
	blockstoreDelay delay.D
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
func NewInstance(ctx context.Context, net tn.Network, router routing.Routing, p tnet.Identity, netOptions []bsnet.NetOpt, bsOptions []bitswap.Option) Instance {
	bsdelay := delay.Fixed(0)

	adapter := net.Adapter(p, netOptions...)
	dstore := ds_sync.MutexWrap(delayed.New(ds.NewMapDatastore(), bsdelay))

	ds := ds_sync.MutexWrap(dstore)
	bstore, err := blockstore.CachedBlockstore(ctx,
		blockstore.NewBlockstore(ds),
		blockstore.DefaultCacheOpts())
	if err != nil {
		panic(err.Error()) // FIXME perhaps change signature and return error.
	}

	bs := bitswap.New(ctx, adapter, router, bstore, bsOptions...)
	return Instance{
		Datastore:       ds,
		Adapter:         adapter,
		Identity:        p,
		Exchange:        bs,
		Routing:         router,
		Blockstore:      bstore,
		blockstoreDelay: bsdelay,
	}
}
