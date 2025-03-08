package examples

import (
	"context"
	"fmt"

	"github.com/ipfs/boxo/examples/unixfs-builder/builder"
)

func AddLargeDirectory() {
	// Create builder with HAMT sharding enabled
	ctx := context.Background()
	opts := builder.DefaultOptions()
	opts.DirSharding = true // Enable HAMT sharding
	opts.ChunkSize = 262144 // 256KiB chunks

	b, err := builder.NewBuilder(ctx, opts)
	if err != nil {
		panic(err)
	}

	// Add progress reporting
	b.WithProgress(func(p builder.Progress) {
		switch p.Operation {
		case "directory":
			fmt.Printf("Adding directory: %s\n", p.Path)
		case "file":
			fmt.Printf("Adding file: %s\n", p.Path)
		}
	})

	// Add a large directory
	if err := b.AddDirectory(ctx, "testdata/large-dir"); err != nil {
		panic(err)
	}

	fmt.Printf("Added large directory with CID: %s\n", b.Root())
}
