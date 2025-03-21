// Package swap provides common interfaces for block-swapping protocols.
package swap

import (
	"context"
	"time"

	"github.com/ipfs/boxo/swap/message"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

// Network defines the the base interface for block swap implementations.
type Network interface {
	// SendMessage sends a Wantlist message to a peer.
	SendMessage(
		context.Context,
		peer.ID,
		message.Wantlist) error

	// Start registers the Reciver and starts handling new messages, connectivity events, etc.
	Start(...Receiver)
	// Stop stops the network service.
	Stop()

	Connect(context.Context, peer.AddrInfo) error
	DisconnectFrom(context.Context, peer.ID) error

	NewMessageSender(context.Context, peer.ID, *MessageSenderOpts) (MessageSender, error)

	Stats() Stats

	Self() peer.ID
	Pinger
	PeerTagger
}

// PeerTagger is an interface for tagging peers with metadata
type PeerTagger interface {
	TagPeer(peer.ID, string, int)
	UntagPeer(peer.ID, string)
	Protect(peer.ID, string)
	Unprotect(peer.ID, string) bool
}

// MessageSender is an interface for sending a series of messages over a swap
// network.
type MessageSender interface {
	SendMsg(context.Context, message.Wantlist) error
	Reset() error
	// Indicates whether the remote peer supports HAVE / DONT_HAVE messages
	SupportsHave() bool
}

type MessageSenderOpts struct {
	MaxRetries       int
	SendTimeout      time.Duration
	SendErrorBackoff time.Duration
}

// Receiver is an interface that can receive messages from the Network.
type Receiver interface {
	ReceiveMessage(
		ctx context.Context,
		sender peer.ID,
		incoming message.Wantlist)

	ReceiveError(error)

	// Connected/Disconnected warns bitswap about peer connections.
	PeerConnected(peer.ID)
	PeerDisconnected(peer.ID)
}

// Pinger is an interface to ping a peer and get the average latency of all pings
type Pinger interface {
	// Ping a peer
	Ping(context.Context, peer.ID) ping.Result
	// Get the average latency of all pings
	Latency(peer.ID) time.Duration
}

// Stats is a container for statistics about the bitswap network
// the numbers inside are specific to bitswap, and not any other protocols
// using the same underlying network.
type Stats struct {
	MessagesSent  uint64
	MessagesRecvd uint64
}
