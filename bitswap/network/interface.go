package network

import (
	"context"

	bsmsg "github.com/ipfs/go-bitswap/message"

	cid "github.com/ipfs/go-cid"
	ifconnmgr "github.com/libp2p/go-libp2p-interface-connmgr"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
)

var (
	// ProtocolBitswapOne is the prefix for the legacy bitswap protocol
	ProtocolBitswapOne protocol.ID = "/ipfs/bitswap/1.0.0"
	// ProtocolBitswapNoVers is equivalent to the legacy bitswap protocol
	ProtocolBitswapNoVers protocol.ID = "/ipfs/bitswap"

	// ProtocolBitswap is the current version of bitswap protocol, 1.1.0
	ProtocolBitswap protocol.ID = "/ipfs/bitswap/1.1.0"
)

// BitSwapNetwork provides network connectivity for BitSwap sessions.
type BitSwapNetwork interface {

	// SendMessage sends a BitSwap message to a peer.
	SendMessage(
		context.Context,
		peer.ID,
		bsmsg.BitSwapMessage) error

	// SetDelegate registers the Reciver to handle messages received from the
	// network.
	SetDelegate(Receiver)

	ConnectTo(context.Context, peer.ID) error

	NewMessageSender(context.Context, peer.ID) (MessageSender, error)

	ConnectionManager() ifconnmgr.ConnManager

	Stats() Stats

	Routing
}

// MessageSender is an interface for sending a series of messages over the bitswap
// network
type MessageSender interface {
	SendMsg(context.Context, bsmsg.BitSwapMessage) error
	Close() error
	Reset() error
}

// Receiver is an interface that can receive messages from the BitSwapNetwork.
type Receiver interface {
	ReceiveMessage(
		ctx context.Context,
		sender peer.ID,
		incoming bsmsg.BitSwapMessage)

	ReceiveError(error)

	// Connected/Disconnected warns bitswap about peer connections.
	PeerConnected(peer.ID)
	PeerDisconnected(peer.ID)
}

// Routing is an interface to providing and finding providers on a bitswap
// network.
type Routing interface {
	// FindProvidersAsync returns a channel of providers for the given key.
	FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.ID

	// Provide provides the key to the network.
	Provide(context.Context, cid.Cid) error
}

// Stats is a container for statistics about the bitswap network
// the numbers inside are specific to bitswap, and not any other protocols
// using the same underlying network.
type Stats struct {
	MessagesSent  uint64
	MessagesRecvd uint64
}
