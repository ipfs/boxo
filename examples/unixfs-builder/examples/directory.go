package examples

import (
	"context"
	"fmt"

	"github.com/ipfs/boxo/examples/unixfs-builder/builder"
)

func AddDirectory() {
	// Create builder with custom options
	ctx := context.Background()
	opts := builder.DefaultOptions()
	opts.PreserveTime = true
	opts.PreserveMode = true

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

	// Add a directory
	if err := b.AddDirectory(ctx, "testdata/example-dir"); err != nil {
		panic(err)
	}

	fmt.Printf("Added directory with CID: %s\n", b.Root())
}
