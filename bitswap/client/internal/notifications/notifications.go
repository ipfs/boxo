package notifications

import (
	"context"
	"sync"
	"time"

	pubsub "github.com/cskr/pubsub"
	"github.com/ipfs/boxo/bitswap/client/traceability"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

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
func New(traceBlock bool) PubSub {
	return &impl{
		wrapped: *pubsub.New(bufferSize),
		closed:  make(chan struct{}),
		trace:   traceBlock,
	}
}

type impl struct {
	lk      sync.RWMutex
	wrapped pubsub.PubSub
	trace   bool

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

	if ps.trace {
		for _, block := range blocks {
			ps.wrapped.Pub(traceability.Block{Block: block, From: from}, block.Cid().KeyString())
		}
	} else {
		for _, block := range blocks {
			ps.wrapped.Pub(block, block.Cid().KeyString())
		}
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

	valuesCh := make(chan any, len(keys)) // provide our own channel to control buffer, prevent blocking

	// prevent shutdown
	ps.lk.RLock()
	defer ps.lk.RUnlock()

	select {
	case <-ps.closed:
		close(blocksCh)
		return blocksCh
	default:
	}

	var subscribe time.Time
	if ps.trace {
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
				var block blocks.Block
				if ps.trace {
					tb := val.(traceability.Block)
					tb.Delay = time.Since(subscribe)
					block = tb
				} else {
					block = val.(blocks.Block)
				}

				select {
				case <-ctx.Done():
					return
				case blocksCh <- block: // continue
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
