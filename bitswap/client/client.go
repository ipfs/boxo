// Package client implements the IPFS exchange interface with the BitSwap
// bilateral exchange protocol.
package client

import (
	"context"
	"time"

	bsnet "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/blockexchange/client"
	"github.com/ipfs/boxo/exchange/blockexchange/tracer"
	"github.com/ipfs/boxo/swap"
	delay "github.com/ipfs/go-ipfs-delay"
	"github.com/libp2p/go-libp2p/core/routing"
)

type DontHaveTimeoutConfig client.DontHaveTimeoutConfig

func DefaultDontHaveTimeoutConfig() *DontHaveTimeoutConfig {
	return (*DontHaveTimeoutConfig)(client.DefaultDontHaveTimeoutConfig())
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/client
// Option defines the functional option type that can be used to configure
// bitswap instances
type Option client.Option

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/client
//
// ProviderSearchDelay sets the initial dely before triggering a provider
// search to find more peers and broadcast the want list. It also partially
// controls re-broadcasts delay when the session idles (does not receive any
// blocks), but these have back-off logic to increase the interval. See
// [defaults.ProvSearchDelay] for the default.
func ProviderSearchDelay(newProvSearchDelay time.Duration) Option {
	return Option(client.ProviderSearchDelay(newProvSearchDelay))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/client
//
// RebroadcastDelay sets a custom delay for periodic search of a random want.
// When the value ellapses, a random CID from the wantlist is chosen and the
// client attempts to find more peers for it and sends them the single want.
// [defaults.RebroadcastDelay] for the default.
func RebroadcastDelay(newRebroadcastDelay delay.D) Option {
	return Option(client.RebroadcastDelay(newRebroadcastDelay))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/client
func SetSimulateDontHavesOnTimeout(send bool) Option {
	return Option(client.SetSimulateDontHavesOnTimeout(send))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/client
func WithDontHaveTimeoutConfig(cfg *DontHaveTimeoutConfig) Option {
	return Option(client.WithDontHaveTimeoutConfig((*client.DontHaveTimeoutConfig)(cfg)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/client
//
// WithPerPeerSendDelay determines how long to wait, based on the number of
// peers, for wants to accumulate before sending a bitswap message to peers. A
// value of 0 uses bitswap messagequeue default.
func WithPerPeerSendDelay(delay time.Duration) Option {
	return Option(client.WithPerPeerSendDelay(delay))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/client
//
// Configures the Client to use given tracer.
// This provides methods to access all messages sent and received by the Client.
// This interface can be used to implement various statistics (this is original intent).
func WithTracer(tap tracer.Tracer) Option {
	return Option(client.WithTracer(tap))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/client
func WithBlockReceivedNotifier(brn BlockReceivedNotifier) Option {
	return Option(client.WithBlockReceivedNotifier(brn))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/client
//
// WithoutDuplicatedBlockStats disable collecting counts of duplicated blocks
// received. This counter requires triggering a blockstore.Has() call for
// every block received by launching goroutines in parallel. In the worst case
// (no caching/blooms etc), this is an expensive call for the datastore to
// answer. In a normal case (caching), this has the power of evicting a
// different block from intermediary caches. In the best case, it doesn't
// affect performance. Use if this stat is not relevant.
func WithoutDuplicatedBlockStats() Option {
	return Option(client.WithoutDuplicatedBlockStats())
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/client
//
// WithDefaultProviderQueryManager indicates whether to use the default
// ProviderQueryManager as a wrapper of the content Router. The default bitswap
// ProviderQueryManager provides bounded parallelism and limits for these
// lookups. The bitswap default ProviderQueryManager uses these options, which
// may be more conservative than the ProviderQueryManager defaults:
//
//   - WithMaxProviders(defaults.BitswapClientDefaultMaxProviders)
//
// To use a custom ProviderQueryManager, set to false and wrap directly the
// content router provided with the WithContentRouting() option. Only takes
// effect if WithContentRouting is set.
func WithDefaultProviderQueryManager(defaultProviderQueryManager bool) Option {
	return Option(client.WithDefaultProviderQueryManager(defaultProviderQueryManager))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/client
type BlockReceivedNotifier client.BlockReceivedNotifier

func convertOpts(in []Option) (out []client.Option) {
	if in == nil {
		return
	}

	out = make([]client.Option, len(in))
	for i, o := range in {
		out[i] = client.Option(o)
	}
	return
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/client
//
// New initializes a Bitswap client that runs until client.Close is called.
// The Content providerFinder paramteter can be nil to disable content-routing
// lookups for content (rely only on bitswap for discovery).
func New(parent context.Context, network bsnet.BitSwapNetwork, providerFinder routing.ContentDiscovery, bstore blockstore.Blockstore, options ...Option) *Client {
	return &Client{
		client.New(parent, swap.Network(network), providerFinder, bstore, convertOpts(options)...),
	}
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/client
// Client instances implement the bitswap protocol.
type Client struct {
	*client.Client
}

// // GetBlock attempts to retrieve a particular block from peers within the
// // deadline enforced by the context.
// // It returns a [github.com/ipfs/boxo/bitswap/client/traceability.Block] assertable [blocks.Block].
// func (bs *Client) GetBlock(ctx context.Context, k cid.Cid) (blocks.Block, error) {
// 	ctx, span := internal.StartSpan(ctx, "GetBlock", trace.WithAttributes(attribute.String("Key", k.String())))
// 	defer span.End()
// 	return bsgetter.SyncGetBlock(ctx, k, bs.GetBlocks)
// }

// // GetBlocks returns a channel where the caller may receive blocks that
// // correspond to the provided |keys|. Returns an error if BitSwap is unable to
// // begin this request within the deadline enforced by the context.
// // It returns a [github.com/ipfs/boxo/bitswap/client/traceability.Block] assertable [blocks.Block].
// //
// // NB: Your request remains open until the context expires. To conserve
// // resources, provide a context with a reasonably short deadline (ie. not one
// // that lasts throughout the lifetime of the server)
// func (bs *Client) GetBlocks(ctx context.Context, keys []cid.Cid) (<-chan blocks.Block, error) {
// 	ctx, span := internal.StartSpan(ctx, "GetBlocks", trace.WithAttributes(attribute.Int("NumKeys", len(keys))))
// 	defer span.End()
// 	session := bs.sm.NewSession(ctx, bs.provSearchDelay, bs.rebroadcastDelay)
// 	return session.GetBlocks(ctx, keys)
// }

// // NotifyNewBlocks announces the existence of blocks to this bitswap service.
// // Bitswap itself doesn't store new blocks. It's the caller responsibility to ensure
// // that those blocks are available in the blockstore before calling this function.
// func (bs *Client) NotifyNewBlocks(ctx context.Context, blks ...blocks.Block) error {
// 	ctx, span := internal.StartSpan(ctx, "NotifyNewBlocks")
// 	defer span.End()

// 	select {
// 	case <-bs.closing:
// 		return errors.New("bitswap is closed")
// 	default:
// 	}

// 	blkCids := make([]cid.Cid, len(blks))
// 	for i, blk := range blks {
// 		blkCids[i] = blk.Cid()
// 	}

// 	// Send all block keys (including duplicates) to any sessions that want them.
// 	// (The duplicates are needed by sessions for accounting purposes)
// 	bs.sm.ReceiveFrom(ctx, "", blkCids, nil, nil)

// 	// Publish the block to any Bitswap clients that had requested blocks.
// 	// (the sessions use this pubsub mechanism to inform clients of incoming
// 	// blocks)
// 	var zero peer.ID
// 	bs.notif.Publish(zero, blks...)

// 	return nil
// }

// // receiveBlocksFrom processes blocks received from the network
// func (bs *Client) receiveBlocksFrom(ctx context.Context, from peer.ID, blks []blocks.Block, haves []cid.Cid, dontHaves []cid.Cid) error {
// 	select {
// 	case <-bs.closing:
// 		return errors.New("bitswap is closed")
// 	default:
// 	}

// 	wanted, notWanted := bs.sim.SplitWantedUnwanted(blks)
// 	if log.Level().Enabled(zapcore.DebugLevel) {
// 		for _, b := range notWanted {
// 			log.Debugf("[recv] block not in wantlist; cid=%s, peer=%s", b.Cid(), from)
// 		}
// 	}

// 	allKs := make([]cid.Cid, 0, len(blks))
// 	for _, b := range blks {
// 		allKs = append(allKs, b.Cid())
// 	}

// 	// Inform the PeerManager so that we can calculate per-peer latency
// 	combined := make([]cid.Cid, 0, len(allKs)+len(haves)+len(dontHaves))
// 	combined = append(combined, allKs...)
// 	combined = append(combined, haves...)
// 	combined = append(combined, dontHaves...)
// 	bs.pm.ResponseReceived(from, combined)

// 	// Send all block keys (including duplicates) to any sessions that want them for accounting purpose.
// 	bs.sm.ReceiveFrom(ctx, from, allKs, haves, dontHaves)

// 	if bs.blockReceivedNotifier != nil {
// 		bs.blockReceivedNotifier.ReceivedBlocks(from, wanted)
// 	}

// 	// Publish the block to any Bitswap clients that had requested blocks.
// 	// (the sessions use this pubsub mechanism to inform clients of incoming
// 	// blocks)
// 	for _, b := range wanted {
// 		bs.notif.Publish(from, b)
// 	}

// 	return nil
// }

// // ReceiveMessage is called by the network interface when a new message is
// // received.
// func (bs *Client) ReceiveMessage(ctx context.Context, p peer.ID, incoming bsmsg.BitSwapMessage) {
// 	bs.counterLk.Lock()
// 	bs.counters.messagesRecvd++
// 	bs.counterLk.Unlock()

// 	if bs.tracer != nil {
// 		bs.tracer.MessageReceived(p, incoming)
// 	}

// 	iblocks := incoming.Blocks()

// 	if len(iblocks) > 0 {
// 		bs.updateReceiveCounters(iblocks)
// 		if log.Level().Enabled(zapcore.DebugLevel) {
// 			for _, b := range iblocks {
// 				log.Debugf("[recv] block; cid=%s, peer=%s", b.Cid(), p)
// 			}
// 		}
// 	}

// 	haves := incoming.Haves()
// 	dontHaves := incoming.DontHaves()
// 	if len(iblocks) > 0 || len(haves) > 0 || len(dontHaves) > 0 {
// 		// Process blocks
// 		err := bs.receiveBlocksFrom(ctx, p, iblocks, haves, dontHaves)
// 		if err != nil {
// 			log.Warnf("ReceiveMessage recvBlockFrom error: %s", err)
// 			return
// 		}
// 	}
// }

// func (bs *Client) updateReceiveCounters(blocks []blocks.Block) {
// 	// Check which blocks are in the datastore
// 	// (Note: any errors from the blockstore are simply logged out in
// 	// blockstoreHas())
// 	var blocksHas []bool
// 	if !bs.skipDuplicatedBlocksStats {
// 		blocksHas = bs.blockstoreHas(blocks)
// 	}

// 	bs.counterLk.Lock()
// 	defer bs.counterLk.Unlock()

// 	// Do some accounting for each block
// 	for i, b := range blocks {
// 		has := (blocksHas != nil) && blocksHas[i]

// 		blkLen := len(b.RawData())
// 		bs.allMetric.Observe(float64(blkLen))
// 		if has {
// 			bs.dupMetric.Observe(float64(blkLen))
// 		}

// 		c := bs.counters

// 		c.blocksRecvd++
// 		c.dataRecvd += uint64(blkLen)
// 		if has {
// 			c.dupBlocksRecvd++
// 			c.dupDataRecvd += uint64(blkLen)
// 		}
// 	}
// }

// func (bs *Client) blockstoreHas(blks []blocks.Block) []bool {
// 	res := make([]bool, len(blks))

// 	wg := sync.WaitGroup{}
// 	for i, block := range blks {
// 		wg.Add(1)
// 		go func(i int, b blocks.Block) {
// 			defer wg.Done()

// 			has, err := bs.blockstore.Has(context.TODO(), b.Cid())
// 			if err != nil {
// 				log.Infof("blockstore.Has error: %s", err)
// 				has = false
// 			}

// 			res[i] = has
// 		}(i, block)
// 	}
// 	wg.Wait()

// 	return res
// }

// // PeerConnected is called by the network interface
// // when a peer initiates a new connection to bitswap.
// func (bs *Client) PeerConnected(p peer.ID) {
// 	bs.pm.Connected(p)
// }

// // PeerDisconnected is called by the network interface when a peer
// // closes a connection
// func (bs *Client) PeerDisconnected(p peer.ID) {
// 	bs.pm.Disconnected(p)
// }

// // ReceiveError is called by the network interface when an error happens
// // at the network layer. Currently just logs error.
// func (bs *Client) ReceiveError(err error) {
// 	log.Infof("Bitswap Client ReceiveError: %s", err)
// 	// TODO log the network error
// 	// TODO bubble the network error up to the parent context/error logger
// }

// // Close is called to shutdown the Client
// func (bs *Client) Close() error {
// 	bs.closeOnce.Do(func() {
// 		close(bs.closing)
// 		bs.sm.Shutdown()
// 		bs.cancel()
// 		if bs.pqm != nil {
// 			bs.pqm.Close()
// 		}
// 		bs.notif.Shutdown()
// 	})
// 	return nil
// }

// // GetWantlist returns the current local wantlist (both want-blocks and
// // want-haves).
// func (bs *Client) GetWantlist() []cid.Cid {
// 	return bs.pm.CurrentWants()
// }

// // GetWantBlocks returns the current list of want-blocks.
// func (bs *Client) GetWantBlocks() []cid.Cid {
// 	return bs.pm.CurrentWantBlocks()
// }

// // GetWanthaves returns the current list of want-haves.
// func (bs *Client) GetWantHaves() []cid.Cid {
// 	return bs.pm.CurrentWantHaves()
// }

// // IsOnline is needed to match go-ipfs-exchange-interface
// func (bs *Client) IsOnline() bool {
// 	return true
// }

// // NewSession generates a new Bitswap session. You should use this, rather
// // that calling Client.GetBlocks, any time you intend to do several related
// // block requests in a row. The session returned will have it's own GetBlocks
// // method, but the session will use the fact that the requests are related to
// // be more efficient in its requests to peers. If you are using a session
// // from blockservice, it will create a bitswap session automatically.
// func (bs *Client) NewSession(ctx context.Context) exchange.Fetcher {
// 	ctx, span := internal.StartSpan(ctx, "NewSession")
// 	defer span.End()
// 	return bs.sm.NewSession(ctx, bs.provSearchDelay, bs.rebroadcastDelay)
// }
