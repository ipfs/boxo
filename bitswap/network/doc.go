// Package network defines interfaces for [Bitswap protocol] network
// operations.
//
// [BitSwapNetwork] is the primary interface, providing message sending, peer
// connectivity management, and connection events. [Receiver] handles incoming
// messages and peer notifications. [MessageSender] supports sending a series
// of messages to a single peer.
//
// [Bitswap protocol]: https://specs.ipfs.tech/bitswap-protocol/
package network
