package blockexchange

import (
	"context"
	"fmt"

	blockstore "github.com/ipfs/boxo/blockstore"
	exchange "github.com/ipfs/boxo/exchange"
	"github.com/ipfs/boxo/exchange/blockexchange/client"
	"github.com/ipfs/boxo/exchange/blockexchange/server"
	"github.com/ipfs/boxo/exchange/blockexchange/tracer"
	"github.com/ipfs/boxo/swap"
	"github.com/ipfs/boxo/swap/message"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-metrics-interface"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.uber.org/multierr"
)

var log = logging.Logger("bitswap")

var (
	_ exchange.SessionExchange = (*BlockExchange)(nil)
)

type BlockExchange struct {
	*client.Client
	*server.Server

	tracer tracer.Tracer
	net    swap.Network
}

func New(ctx context.Context, net swap.Network, providerFinder routing.ContentDiscovery, bstore blockstore.Blockstore, options ...Option) *BlockExchange {
	bs := &BlockExchange{
		net: net,
	}

	var serverOptions []server.Option
	var clientOptions []client.Option

	for _, o := range options {
		switch typedOption := o.v.(type) {
		case server.Option:
			serverOptions = append(serverOptions, typedOption)
		case client.Option:
			clientOptions = append(clientOptions, typedOption)
		case option:
			typedOption(bs)
		default:
			panic(fmt.Errorf("unknown option type passed to blockExchange.New, got: %T, %v; expected: %T, %T or %T", typedOption, typedOption, server.Option(nil), client.Option(nil), option(nil)))
		}
	}

	if bs.tracer != nil {
		var tracer tracer.Tracer = nopReceiveTracer{bs.tracer}
		clientOptions = append(clientOptions, client.WithTracer(tracer))
		serverOptions = append(serverOptions, server.WithTracer(tracer))
	}

	ctx = metrics.CtxSubScope(ctx, "bitswap")

	bs.Server = server.New(ctx, net, bstore, serverOptions...)
	bs.Client = client.New(ctx, net, providerFinder, bstore, append(clientOptions, client.WithBlockReceivedNotifier(bs.Server))...)
	net.Start(bs) // use the polyfill receiver to log received errors and trace messages only once

	return bs
}

func (bs *BlockExchange) NotifyNewBlocks(ctx context.Context, blks ...blocks.Block) error {
	return multierr.Combine(
		bs.Client.NotifyNewBlocks(ctx, blks...),
		bs.Server.NotifyNewBlocks(ctx, blks...),
	)
}

type Stat struct {
	Wantlist         []cid.Cid
	Peers            []string
	BlocksReceived   uint64
	DataReceived     uint64
	DupBlksReceived  uint64
	DupDataReceived  uint64
	MessagesReceived uint64
	BlocksSent       uint64
	DataSent         uint64
}

func (bs *BlockExchange) Stat() (*Stat, error) {
	cs, err := bs.Client.Stat()
	if err != nil {
		return nil, err
	}
	ss, err := bs.Server.Stat()
	if err != nil {
		return nil, err
	}

	return &Stat{
		Wantlist:         cs.Wantlist,
		BlocksReceived:   cs.BlocksReceived,
		DataReceived:     cs.DataReceived,
		DupBlksReceived:  cs.DupBlksReceived,
		DupDataReceived:  cs.DupDataReceived,
		MessagesReceived: cs.MessagesReceived,
		Peers:            ss.Peers,
		BlocksSent:       ss.BlocksSent,
		DataSent:         ss.DataSent,
	}, nil
}

func (bs *BlockExchange) Close() error {
	bs.net.Stop()
	bs.Client.Close()
	bs.Server.Close()
	return nil
}

func (bs *BlockExchange) WantlistForPeer(p peer.ID) []cid.Cid {
	if p == bs.net.Self() {
		return bs.Client.GetWantlist()
	}
	return bs.Server.WantlistForPeer(p)
}

func (bs *BlockExchange) PeerConnected(p peer.ID) {
	bs.Client.PeerConnected(p)
	bs.Server.PeerConnected(p)
}

func (bs *BlockExchange) PeerDisconnected(p peer.ID) {
	bs.Client.PeerDisconnected(p)
	bs.Server.PeerDisconnected(p)
}

func (bs *BlockExchange) ReceiveError(err error) {
	log.Infof("BlockExchange Client ReceiveError: %s", err)
	// TODO log the network error
	// TODO bubble the network error up to the parent context/error logger
}

func (bs *BlockExchange) ReceiveMessage(ctx context.Context, p peer.ID, incoming message.Wantlist) {
	if bs.tracer != nil {
		bs.tracer.MessageReceived(p, incoming)
	}

	bs.Client.ReceiveMessage(ctx, p, incoming)
	bs.Server.ReceiveMessage(ctx, p, incoming)
}
