package network

import (
	"context"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"go.uber.org/multierr"
)

type router struct {
	Bitswap BitSwapNetwork
	HTTP    BitSwapNetwork
}

// New returns a BitSwapNetwork supported by underlying IPFS host.
func New(bitswap BitSwapNetwork, http BitSwapNetwork) BitSwapNetwork {
	return &router{
		Bitswap: bitswap,
		HTTP:    http,
	}
}

func (rt *router) Start(receivers ...Receiver) {
	rt.Bitswap.Start(receivers...)
	rt.HTTP.Start(receivers...)
}

func (rt *router) Stop() {
	rt.Bitswap.Stop()
	rt.HTTP.Stop()
}

func (rt *router) Ping(ctx context.Context, p peer.AddrInfo) ping.Result {
	htaddrs, bsaddrs := SplitHTTPAddrs(p)
	if len(htaddrs.Addrs) > 0 {
		return rt.HTTP.Ping(ctx, htaddrs)
	}
	return rt.Bitswap.Ping(ctx, bsaddrs)
}

// TODO
func (rt *router) Latency(p peer.AddrInfo) time.Duration {
	htaddrs, bsaddrs := SplitHTTPAddrs(p)
	if len(htaddrs.Addrs) > 0 {
		return rt.HTTP.Latency(htaddrs)
	}
	return rt.Bitswap.Latency(bsaddrs)
}

func (rt *router) SendMessage(ctx context.Context, p peer.AddrInfo, msg bsmsg.BitSwapMessage) error {
	htaddrs, bsaddrs := SplitHTTPAddrs(p)
	return multierr.Combine(
		rt.HTTP.SendMessage(ctx, htaddrs, msg),
		rt.Bitswap.SendMessage(ctx, bsaddrs, msg),
	)
}

func (rt *router) Connect(ctx context.Context, p peer.AddrInfo) error {
	htaddrs, bsaddrs := SplitHTTPAddrs(p)
	return multierr.Combine(
		rt.HTTP.Connect(ctx, htaddrs),
		rt.Bitswap.Connect(ctx, bsaddrs),
	)
}

func (rt *router) DisconnectFrom(ctx context.Context, p peer.ID) error {
	return multierr.Combine(
		rt.HTTP.DisconnectFrom(ctx, p),
		rt.Bitswap.DisconnectFrom(ctx, p),
	)
}

func (rt *router) Stats() Stats {
	htstats := rt.HTTP.Stats()
	bsstats := rt.Bitswap.Stats()
	return Stats{
		MessagesRecvd: htstats.MessagesRecvd + bsstats.MessagesRecvd,
		MessagesSent:  htstats.MessagesSent + bsstats.MessagesSent,
	}
}

func (rt *router) NewMessageSender(ctx context.Context, p peer.AddrInfo, opts *MessageSenderOpts) (MessageSender, error) {
	htaddrs, bsaddrs := SplitHTTPAddrs(p)
	if len(htaddrs.Addrs) > 0 {
		return rt.HTTP.NewMessageSender(ctx, htaddrs, opts)
	}
	return rt.Bitswap.NewMessageSender(ctx, bsaddrs, opts)
}

func (rt *router) TagPeer(p peer.AddrInfo, tag string, w int) {
	rt.HTTP.TagPeer(p, tag, w)
	rt.Bitswap.TagPeer(p, tag, w)
}
func (rt *router) UntagPeer(p peer.AddrInfo, tag string) {
	rt.HTTP.UntagPeer(p, tag)
	rt.Bitswap.UntagPeer(p, tag)
}

func (rt *router) Protect(p peer.AddrInfo, tag string) {
	rt.HTTP.Protect(p, tag)
	rt.Bitswap.Protect(p, tag)
}
func (rt *router) Unprotect(p peer.AddrInfo, tag string) bool {
	return rt.HTTP.Unprotect(p, tag) || rt.Bitswap.Unprotect(p, tag) // FIXME
}
