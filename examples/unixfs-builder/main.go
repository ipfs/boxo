package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/ipfs/boxo/examples/unixfs-builder/builder"
)

func main() {
	// Parse command line flags
	chunkSize := flag.Int64("chunk-size", 256*1024, "Chunk size in bytes")
	preserveTime := flag.Bool("preserve-time", false, "Preserve modification times")
	recursive := flag.Bool("r", false, "Add directory recursively")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Usage: unixfs-builder [options] <path>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Create builder options
	opts := builder.DefaultOptions()
	opts.ChunkSize = *chunkSize
	opts.PreserveTime = *preserveTime

	// Create builder
	ctx := context.Background()
	b, err := builder.NewBuilder(ctx, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create builder: %s\n", err)
		os.Exit(1)
	}

	// Add progress reporting
	b.WithProgress(func(p builder.Progress) {
		switch p.Operation {
		case "file":
			fmt.Printf("Adding file: %s (%s)\n", p.Path,
				builder.HumanReadableSize(p.TotalBytes))
		case "chunk":
			percent := float64(p.BytesProcessed) / float64(p.TotalBytes) * 100
			fmt.Printf("\rProgress: %.1f%% (%s/%s)", percent,
				builder.HumanReadableSize(p.BytesProcessed),
				builder.HumanReadableSize(p.TotalBytes))
		case "directory":
			fmt.Printf("Adding directory: %s\n", p.Path)
		}
	})

	// Get the target path
	path := flag.Arg(0)

	// Get file/directory info
	info, err := os.Stat(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to stat path: %s\n", err)
		os.Exit(1)
	}

	// Handle based on whether it's a file or directory
	if info.IsDir() {
		if !*recursive {
			fmt.Fprintf(os.Stderr, "Path is a directory. Use -r flag to add recursively\n")
			os.Exit(1)
		}
		err = b.AddDirectory(ctx, path)
	} else {
		err = b.AddFile(ctx, path)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add path: %s\n", err)
		os.Exit(1)
	}

	// Print the root CID
	fmt.Printf("Added %s: %s\n", path, b.Root())
}
