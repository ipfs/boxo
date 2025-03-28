package bitswap

import (
	"time"

	bsclient "github.com/ipfs/boxo/bitswap/client"
	bsserver "github.com/ipfs/boxo/bitswap/server"
	bstracer "github.com/ipfs/boxo/bitswap/tracer"
	"github.com/ipfs/boxo/exchange/blockexchange"
	"github.com/ipfs/boxo/exchange/blockexchange/client"
	"github.com/ipfs/boxo/exchange/blockexchange/server"
	"github.com/ipfs/boxo/exchange/blockexchange/tracer"
	delay "github.com/ipfs/go-ipfs-delay"
)

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
type Option blockexchange.Option

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func EngineBlockstoreWorkerCount(count int) Option {
	return Option(blockexchange.WithServerOption(server.EngineBlockstoreWorkerCount(count)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func EngineTaskWorkerCount(count int) Option {
	return Option(blockexchange.WithServerOption(server.EngineTaskWorkerCount(count)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func MaxOutstandingBytesPerPeer(count int) Option {
	return Option(blockexchange.WithServerOption(server.MaxOutstandingBytesPerPeer(count)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func MaxQueuedWantlistEntriesPerPeer(count uint) Option {
	return Option(blockexchange.WithServerOption(server.MaxQueuedWantlistEntriesPerPeer(count)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
// MaxCidSize only affects the server.
// If it is 0 no limit is applied.
func MaxCidSize(n uint) Option {
	return Option(blockexchange.WithServerOption(server.MaxCidSize(n)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func TaskWorkerCount(count int) Option {
	return Option(blockexchange.WithServerOption(server.TaskWorkerCount(count)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func SetSendDontHaves(send bool) Option {
	return Option(blockexchange.WithServerOption(server.SetSendDontHaves(send)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func WithPeerBlockRequestFilter(pbrf server.PeerBlockRequestFilter) Option {
	return Option(blockexchange.WithServerOption(server.WithPeerBlockRequestFilter(pbrf)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func WithScoreLedger(scoreLedger server.ScoreLedger) Option {
	return Option(blockexchange.WithServerOption(server.WithScoreLedger(scoreLedger)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func WithPeerLedger(peerLedger server.PeerLedger) Option {
	return Option(blockexchange.WithServerOption(server.WithPeerLedger(peerLedger)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func WithTargetMessageSize(tms int) Option {
	return Option(blockexchange.WithServerOption(server.WithTargetMessageSize(tms)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func WithTaskComparator(comparator server.TaskComparator) Option {
	return Option(blockexchange.WithServerOption(server.WithTaskComparator(comparator)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
//
// WithWantHaveReplaceSize sets the maximum size of a block in bytes up to
// which the bitswap server will replace a WantHave with a WantBlock response.
// See [server.WithWantHaveReplaceSize] for details.
func WithWantHaveReplaceSize(size int) Option {
	return Option(blockexchange.WithServerOption(server.WithWantHaveReplaceSize(size)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func ProviderSearchDelay(newProvSearchDelay time.Duration) Option {
	return Option(blockexchange.WithClientOption(client.ProviderSearchDelay(newProvSearchDelay)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func RebroadcastDelay(newRebroadcastDelay delay.D) blockexchange.Option {
	return blockexchange.RebroadcastDelay(newRebroadcastDelay)
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func SetSimulateDontHavesOnTimeout(send bool) Option {
	return Option(blockexchange.WithClientOption(client.SetSimulateDontHavesOnTimeout(send)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func WithBlockReceivedNotifier(brn client.BlockReceivedNotifier) Option {
	return Option(blockexchange.WithClientOption(client.WithBlockReceivedNotifier(brn)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func WithoutDuplicatedBlockStats() Option {
	return Option(blockexchange.WithClientOption(client.WithoutDuplicatedBlockStats()))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func WithTracer(tap bstracer.Tracer) Option {
	return Option(blockexchange.WithTracer(tracer.Tracer(tap)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func WithClientOption(opt bsclient.Option) Option {
	o := blockexchange.Option{}
	o.Set(client.Option(opt))
	return Option(o)
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange
func WithServerOption(opt bsserver.Option) Option {
	o := blockexchange.Option{}
	o.Set(server.Option(opt))
	return Option(o)
}

func convertOpts(in []Option) (out []blockexchange.Option) {
	if in == nil {
		return
	}

	out = make([]blockexchange.Option, len(in))
	for i, o := range in {
		out[i] = blockexchange.Option(o)
	}
	return
}
