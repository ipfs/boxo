// Package bitswap implements the [Bitswap protocol] for exchanging blocks
// between IPFS peers.
//
// [Bitswap] combines a [client.Client] for requesting blocks and a
// [server.Server] for serving them. Create instances with [New], which
// accepts both client and server options.
//
//	bs := bitswap.New(ctx, network, providerFinder, blockstore)
//	defer bs.Close()
//
//	block, err := bs.GetBlock(ctx, c)
//
// [Bitswap protocol]: https://specs.ipfs.tech/bitswap-protocol/
package bitswap
