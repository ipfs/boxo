package server

import (
	"context"

	bsnet "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/bitswap/tracer"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/blockexchange/server"
	"github.com/ipfs/boxo/swap"
)

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
type Option server.Option

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
type Server struct {
	*server.Server
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
func New(ctx context.Context, network bsnet.BitSwapNetwork, bstore blockstore.Blockstore, options ...Option) *Server {
	return &Server{
		server.New(ctx, swap.Network(network), bstore, convertOpts(options)...),
	}
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
func TaskWorkerCount(count int) Option {
	return Option(server.TaskWorkerCount(count))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
func WithTracer(tap tracer.Tracer) Option {
	return Option(server.WithTracer(tap))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
func WithPeerBlockRequestFilter(pbrf PeerBlockRequestFilter) Option {
	return Option(server.WithPeerBlockRequestFilter(server.PeerBlockRequestFilter(pbrf)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
//
// WithTaskComparator configures custom task prioritization logic.
func WithTaskComparator(comparator TaskComparator) Option {
	return Option(server.WithTaskComparator(server.TaskComparator(comparator)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
//
// Configures the engine to use the given score decision logic.
func WithScoreLedger(scoreLedger ScoreLedger) Option {
	return Option(server.WithScoreLedger(server.ScoreLedger(scoreLedger)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
//
// WithPeerLedger configures the engine with a custom [decision.PeerLedger].
func WithPeerLedger(peerLedger PeerLedger) Option {
	return Option(server.WithPeerLedger(server.PeerLedger(peerLedger)))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
//
// EngineTaskWorkerCount sets the number of worker threads used inside the engine
func EngineTaskWorkerCount(count int) Option {
	return Option(server.EngineTaskWorkerCount(count))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
//
// SetSendDontHaves indicates what to do when the engine receives a want-block
// for a block that is not in the blockstore. Either
// - Send a DONT_HAVE message
// - Simply don't respond
// This option is only used for testing.
func SetSendDontHaves(send bool) Option {
	return Option(server.SetSendDontHaves(send))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
//
// EngineBlockstoreWorkerCount sets the number of worker threads used for
// blockstore operations in the decision engine
func EngineBlockstoreWorkerCount(count int) Option {
	return Option(server.EngineBlockstoreWorkerCount(count))
}

func WithTargetMessageSize(tms int) Option {
	return Option(server.WithTargetMessageSize(tms))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
//
// MaxOutstandingBytesPerPeer describes approximately how much work we are will to have outstanding to a peer at any
// given time. Setting it to 0 will disable any limiting.
func MaxOutstandingBytesPerPeer(count int) Option {
	return Option(server.MaxOutstandingBytesPerPeer(count))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
//
// MaxQueuedWantlistEntriesPerPeer limits how much individual entries each peer is allowed to send.
// If a peer send us more than this we will truncate newest entries.
// It defaults to defaults.MaxQueuedWantlistEntiresPerPeer.
func MaxQueuedWantlistEntriesPerPeer(count uint) Option {
	return Option(server.MaxQueuedWantlistEntriesPerPeer(count))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
//
// MaxCidSize limits how big CIDs we are willing to serve.
// We will ignore CIDs over this limit.
// It defaults to [defaults.MaxCidSize].
// If it is 0 no limit is applied.
func MaxCidSize(n uint) Option {
	return Option(server.MaxCidSize(n))
}

// Deprecated: use github.com/ipfs/boxo/exchange/blockexchange/server
//
// WithWantHaveReplaceSize sets the maximum size of a block in bytes up to
// which the bitswap server will replace a WantHave with a WantBlock response.
//
// Behavior:
//   - If size > 0: The server may send full blocks instead of just confirming possession
//     for blocks up to the specified size.
//   - If size = 0: WantHave replacement is disabled entirely. This allows the server to
//     skip reading block sizes during WantHave request processing, which can be more
//     efficient if the data storage bills "possession" checks and "reads" differently.
//
// Performance considerations:
//   - Enabling replacement (size > 0) may reduce network round-trips but requires
//     checking block sizes for each WantHave request to decide if replacement should occur.
//   - Disabling replacement (size = 0) optimizes server performance by avoiding
//     block size checks, potentially reducing infrastructure costs if possession checks
//     are less expensive than full reads.
//
// It defaults to [defaults.DefaultWantHaveReplaceSize]
// and the value may change in future releases.
//
// Use this option to set explicit behavior to balance between network
// efficiency, server performance, and potential storage cost optimizations
// based on your specific use case and storage backend.
func WithWantHaveReplaceSize(size int) Option {
	return Option(server.WithWantHaveReplaceSize(size))
}

func convertOpts(in []Option) (out []server.Option) {
	if in == nil {
		return
	}

	out = make([]server.Option, len(in))
	for i, o := range in {
		out[i] = server.Option(o)
	}
	return
}
