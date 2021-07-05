// package blockstore implements IPFS blockstore interface backed by a CAR file.
// This package provides two flavours of blockstore: ReadOnly and ReadWrite.
//
// The ReadOnly blockstore provides a read-only random access from a given data payload either in
// unindexed v1 format or indexed/unindexed v2 format:
// * ReadOnly.NewReadOnly can be used to instantiate a new read-only blockstore for a given CAR v1
//   or CAR v2 data payload with an optional index override.
// * ReadOnly.OpenReadOnly can be used to instantiate a new read-only blockstore for a given CAR v2
//    or car v2 file with automatic index generation if the index is not present.

package blockstore
