package network

import (
	"context"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"

	cid "github.com/ipfs/go-cid"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

// BitSwapNetwork provides network connectivity for BitSwap sessions.
type BitSwapNetwork interface {
	// SendMessage sends a BitSwap message to a peer.
	SendMessage(
		context.Context,
		peer.AddrInfo,
		bsmsg.BitSwapMessage) error

	// Start registers the Reciver and starts handling new messages, connectivity events, etc.
	Start(...Receiver)
	// Stop stops the network service.
	Stop()

	Connect(context.Context, peer.AddrInfo) error
	DisconnectFrom(context.Context, peer.ID) error

	NewMessageSender(context.Context, peer.AddrInfo, *MessageSenderOpts) (MessageSender, error)

	Stats() Stats

	Pinger
	TagPeer(peer.AddrInfo, string, int)
	UntagPeer(peer.AddrInfo, string)
	Protect(peer.AddrInfo, string)
	Unprotect(peer.AddrInfo, string) bool
}

// MessageSender is an interface for sending a series of messages over the bitswap
// network
type MessageSender interface {
	SendMsg(context.Context, bsmsg.BitSwapMessage) error
	Close() error
	Reset() error
	// Indicates whether the remote peer supports HAVE / DONT_HAVE messages
	SupportsHave() bool
}

type MessageSenderOpts struct {
	MaxRetries       int
	SendTimeout      time.Duration
	SendErrorBackoff time.Duration
}

// Receiver is an interface that can receive messages from the BitSwapNetwork.
type Receiver interface {
	ReceiveMessage(
		ctx context.Context,
		sender peer.AddrInfo,
		incoming bsmsg.BitSwapMessage)

	ReceiveError(error)

	// Connected/Disconnected warns bitswap about peer connections.
	PeerConnected(peer.AddrInfo)
	PeerDisconnected(peer.AddrInfo)
}

// Routing is an interface to providing and finding providers on a bitswap
// network.
type Routing interface {
	// FindProvidersAsync returns a channel of providers for the given key.
	FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo

	// Provide provides the key to the network.
	Provide(context.Context, cid.Cid) error
}

// Pinger is an interface to ping a peer and get the average latency of all pings
type Pinger interface {
	// Ping a peer
	Ping(context.Context, peer.AddrInfo) ping.Result
	// Get the average latency of all pings
	Latency(peer.AddrInfo) time.Duration
}

// Stats is a container for statistics about the bitswap network
// the numbers inside are specific to bitswap, and not any other protocols
// using the same underlying network.
type Stats struct {
	MessagesSent  uint64
	MessagesRecvd uint64
}
