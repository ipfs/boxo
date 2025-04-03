// File: examples/unixfs-builder/examples/single_file.go

package examples

import (
	"context"
	"fmt"

	"github.com/ipfs/boxo/examples/unixfs-builder/builder"
)

func AddSingleFile() {
	// Create a new builder with default options
	ctx := context.Background()
	b, err := builder.NewBuilder(ctx, builder.DefaultOptions())
	if err != nil {
		panic(err)
	}

	// Add progress reporting
	b.WithProgress(func(p builder.Progress) {
		switch p.Operation {
		case "file":
			fmt.Printf("Adding: %s\n", p.Path)
		case "chunk":
			percent := float64(p.BytesProcessed) / float64(p.TotalBytes) * 100
			fmt.Printf("\rProgress: %.1f%%", percent)
		}
	})

	// Add a file
	if err := b.AddFile(ctx, "testdata/hello.txt"); err != nil {
		panic(err)
	}

	fmt.Printf("\nAdded file with CID: %s\n", b.Root())
}
