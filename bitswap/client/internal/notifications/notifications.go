package notifications

import (
	"context"
	"sync"
	"time"

	pubsub "github.com/cskr/pubsub"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap/zapcore"
)

var log = logging.Logger("bitswap/client/notify")

const bufferSize = 16

// PubSub is a simple interface for publishing blocks and being able to subscribe
// for cids. It's used internally by bitswap to decouple receiving blocks
// and actually providing them back to the GetBlocks caller.
type PubSub interface {
	Publish(from peer.ID, blocks ...blocks.Block)
	Subscribe(ctx context.Context, keys ...cid.Cid) <-chan blocks.Block
	Shutdown()
}

// New generates a new PubSub interface.
func New() PubSub {
	return &impl{
		wrapped: *pubsub.New(bufferSize),
		closed:  make(chan struct{}),
	}
}

// notifyBlock is a block whose provenance is tracked.
type notifyBlock struct {
	blocks.Block

	// from contains the peer id of the node who sent us the block.
	// It will be the zero value if we did not downloaded this block from the
	// network. (such as by getting the block from NotifyNewBlocks).
	from peer.ID
}

type impl struct {
	lk      sync.RWMutex
	wrapped pubsub.PubSub

	closed chan struct{}
}

func (ps *impl) Publish(from peer.ID, blocks ...blocks.Block) {
	ps.lk.RLock()
	defer ps.lk.RUnlock()
	select {
	case <-ps.closed:
		return
	default:
	}

	for _, block := range blocks {
		ps.wrapped.Pub(notifyBlock{Block: block, from: from}, block.Cid().KeyString())
	}
}

func (ps *impl) Shutdown() {
	ps.lk.Lock()
	defer ps.lk.Unlock()
	select {
	case <-ps.closed:
		return
	default:
	}
	close(ps.closed)
	ps.wrapped.Shutdown()
}

// Subscribe returns a channel of blocks for the given |keys|. |blockChannel|
// is closed if the |ctx| times out or is cancelled, or after receiving the blocks
// corresponding to |keys|.
func (ps *impl) Subscribe(ctx context.Context, keys ...cid.Cid) <-chan blocks.Block {
	blocksCh := make(chan blocks.Block, len(keys))
	if len(keys) == 0 {
		close(blocksCh)
		return blocksCh
	}

	valuesCh := make(chan interface{}, len(keys)) // provide our own channel to control buffer, prevent blocking

	// prevent shutdown
	ps.lk.RLock()
	defer ps.lk.RUnlock()

	select {
	case <-ps.closed:
		close(blocksCh)
		return blocksCh
	default:
	}

	logDebug := log.Level().Enabled(zapcore.DebugLevel)

	var subscribe time.Time
	if logDebug {
		subscribe = time.Now()
	}

	// AddSubOnceEach listens for each key in the list, and closes the channel
	// once all keys have been received
	ps.wrapped.AddSubOnceEach(valuesCh, toStrings(keys)...)
	go func() {
		defer func() {
			close(blocksCh)

			ps.lk.RLock()
			defer ps.lk.RUnlock()
			// Don't touch the pubsub instance if we're
			// already closed.
			select {
			case <-ps.closed:
				return
			default:
			}

			ps.wrapped.Unsub(valuesCh)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ps.closed:
			case val, ok := <-valuesCh:
				if !ok {
					return
				}
				block, ok := val.(notifyBlock)
				if !ok {
					panic("published item was not a notifyBlock")
				}

				if logDebug {
					log.Debugw("received block from peer", "cid", block.Cid(), "from", block.from.String(), "delay", time.Since(subscribe).String())
				}

				select {
				case <-ctx.Done():
					return
				case blocksCh <- block.Block: // continue
				case <-ps.closed:
				}
			}
		}
	}()

	return blocksCh
}

func toStrings(keys []cid.Cid) []string {
	strs := make([]string, 0, len(keys))
	for _, key := range keys {
		strs = append(strs, key.KeyString())
	}
	return strs
}
