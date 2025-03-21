package blockexchange

import (
	"github.com/ipfs/boxo/exchange/blockexchange/tracer"
	"github.com/ipfs/boxo/swap/message"
	"github.com/libp2p/go-libp2p/core/peer"
)

type sendOnlyTracer interface {
	MessageSent(peer.ID, message.Wantlist)
}

var _ tracer.Tracer = nopReceiveTracer{}

// we need to only trace sends because we already trace receives in the polyfill object (to not get them traced twice)
type nopReceiveTracer struct {
	sendOnlyTracer
}

func (nopReceiveTracer) MessageReceived(peer.ID, message.Wantlist) {}
