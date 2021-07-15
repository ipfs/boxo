// package blockstore implements IPFS blockstore interface backed by a CAR file.
// This package provides two flavours of blockstore: ReadOnly and ReadWrite.
//
// The ReadOnly blockstore provides a read-only random access from a given data payload either in
// unindexed v1 format or indexed/unindexed v2 format:
// * ReadOnly.NewReadOnly can be used to instantiate a new read-only blockstore for a given CAR v1
//   or CAR v2 data payload with an optional index override.
// * ReadOnly.OpenReadOnly can be used to instantiate a new read-only blockstore for a given CAR v1
//    or CAR v2 file with automatic index generation if the index is not present.
//
// The ReadWrite blockstore allows writing and reading of the blocks concurrently. The user of this
// blockstore is responsible for calling ReadWrite.Finalize when finished writing blocks.
// Upon finalization, the instance can no longer be used for reading or writing blocks and will
// panic if used. To continue reading the blocks users are encouraged to use ReadOnly blockstore
// instantiated from the same file path using OpenReadOnly.
// A user may resume reading/writing from files produced by an instance of ReadWrite blockstore. The
// resumption is attempted automatically, if the path passed to OpenReadWrite exists.
package blockstore
