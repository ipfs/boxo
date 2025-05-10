package builder

import (
	"github.com/multiformats/go-multihash"
)

type Options struct {
	// ChunkSize is the maximum size of leaf nodes in bytes
	ChunkSize int64

	// HashFunc is the hash function to use for generating CIDs
	// Default: sha2-256
	HashFunc uint64

	// Chunker specifies how to split input data
	// Options: "size", "rabin"
	// Default: "size"
	Chunker string

	// PreserveTime if true, keeps original mtime metadata
	PreserveTime bool

	// PreserveMode if true, keeps original file mode (permissions)
	PreserveMode bool

	// DirSharding enables HAMT-based directory sharding for large directories
	DirSharding bool
}

// DefaultOptions returns the default builder options
func DefaultOptions() Options {
	return Options{
		ChunkSize:    256 * 1024, // 256KiB chunks, same as 'ipfs add' default
		HashFunc:     multihash.SHA2_256,
		Chunker:      "size",
		PreserveTime: false,
		DirSharding:  false,
	}
}
