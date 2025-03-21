package message

import "github.com/ipfs/boxo/swap/message"

// Deprecated: use github.com/ipfs/boxo/swap/message
// BitSwapMessage is the basic interface for interacting building, encoding,
// and decoding messages sent on the BitSwap protocol.
type BitSwapMessage message.Wantlist

// Deprecated: use github.com/ipfs/boxo/swap/message
// Entry is a wantlist entry in a Bitswap message, with flags indicating
// - whether message is a cancel
// - whether requester wants a DONT_HAVE message
// - whether requester wants a HAVE message (instead of the block)
type Entry message.Entry
