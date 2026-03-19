// Package message implements [Bitswap protocol] messages.
//
// A [BitSwapMessage] carries wantlist entries, blocks, and block presence
// indicators (HAVE/DONT_HAVE) between peers. Messages are serialized using
// Protocol Buffers for network transmission. Use [New] to create a message and
// [FromNet] to decode one from a network stream.
//
// [Bitswap protocol]: https://specs.ipfs.tech/bitswap-protocol/
package message
