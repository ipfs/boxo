package bitswap

import (
	bsmsg "github.com/ipfs/go-bitswap/message"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// WireTap provides methods to access all messages sent and received by Bitswap.
// This interface can be used to implement various statistics (this is original intent).
type WireTap interface {
	MessageReceived(peer.ID, bsmsg.BitSwapMessage)
	MessageSent(peer.ID, bsmsg.BitSwapMessage)
}

// Configures Bitswap to use given wiretap.
func EnableWireTap(tap WireTap) Option {
	return func(bs *Bitswap) {
		bs.wiretap = tap
	}
}

// Configures Bitswap not to use any wiretap.
func DisableWireTap() Option {
	return func(bs *Bitswap) {
		bs.wiretap = nil
	}
}
