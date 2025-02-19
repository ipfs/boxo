package builder

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	chunker "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
)

// Builder handles the creation of UnixFS DAGs
type Builder struct {
	opts Options
	dag  ipld.DAGService
	root cid.Cid
	size int64
}

// Root returns the root CID of the built UnixFS DAG
func (b *Builder) Root() cid.Cid {
	return b.root
}

// Size returns the total size of the data
func (b *Builder) Size() int64 {
	return b.size
}

// NewBuilder creates a new UnixFS builder with the given options
func NewBuilder(ctx context.Context, opts Options) (*Builder, error) {
	// Create an in-memory DAG service
	dstore := dssync.MutexWrap(ds.NewMapDatastore())

	// Create a blockstore using datastore
	bs := blockstore.NewBlockstore(dstore)

	// Create a blockservice
	bserv := blockservice.New(bs, nil)

	// Create a DAG service (boxo merkledag)
	dagService := merkledag.NewDAGService(bserv)

	return &Builder{
		opts: opts,
		dag:  dagService,
	}, nil
}

// AddFile adds a single file to the UnixFS DAG
func (b *Builder) AddFile(ctx context.Context, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file info for metadata
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	// Create a chunker based on options
	chunker := b.createChunker(file)

	// Create UnixFS DAG params
	params := helpers.DagBuilderParams{
		Maxlinks:   helpers.DefaultLinksPerBlock,
		RawLeaves:  true,
		CidBuilder: cid.V1Builder{Codec: cid.DagProtobuf, MhType: b.opts.HashFunc},
		Dagserv:    b.dag,
	}

	// Create DAG builder
	db, err := params.New(chunker)
	if err != nil {
		return fmt.Errorf("failed to create DAG builder: %w", err)
	}

	// Build balanced DAG
	node, err := balanced.Layout(db)
	if err != nil {
		return fmt.Errorf("failed to build DAG: %w", err)
	}

	// Store the root CID
	b.root = node.Cid()
	b.size = info.Size()

	return nil
}

// createChunker creates a chunker based on builder options
func (b *Builder) createChunker(r io.Reader) chunker.Splitter {
	switch b.opts.Chunker {
	case "rabin":
		return chunker.NewRabin(r, uint64(b.opts.ChunkSize))
	default: // "size"
		return chunker.NewSizeSplitter(r, int64(b.opts.ChunkSize))
	}
}

// AddDirectory adds a directory and its contents to the UnixFS DAG
func (b *Builder) AddDirectory(ctx context.Context, path string) error {
	return filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories - we'll create them as we add files
		if info.IsDir() {
			return nil
		}

		// Get the relative path from the root directory
		relPath, err := filepath.Rel(path, filePath)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}

		// Add the file
		if err := b.AddFile(ctx, filePath); err != nil {
			return fmt.Errorf("failed to add file %s: %w", relPath, err)
		}

		fmt.Printf("Added %s\n", relPath)
		return nil
	})
}
