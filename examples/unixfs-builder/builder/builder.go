package builder

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	chunker "github.com/ipfs/boxo/chunker"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/ipld/unixfs/hamt"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	format "github.com/ipfs/go-ipld-format"
)

type Builder struct {
	opts        Options
	dagService  format.DAGService
	root        cid.Cid
	size        int64
	directories map[string]*dag.ProtoNode
	onProgress  ProgressFunc
}

func (b *Builder) Root() cid.Cid {
	return b.root
}

func (b *Builder) WithProgress(fn ProgressFunc) {
	b.onProgress = fn
}

func (b *Builder) reportProgress(p Progress) {
	if b.onProgress != nil {
		b.onProgress(p)
	}
}

// NewBuilder creates a new UnixFS builder with the given options
func NewBuilder(ctx context.Context, opts Options) (*Builder, error) {
	// Previous initialization code...
	dstore := dssync.MutexWrap(ds.NewMapDatastore())

	// Create a blockstore using datastore
	bs := blockstore.NewBlockstore(dstore)

	// Create a blockservice
	bserv := blockservice.New(bs, nil)

	// Create a DAG service (boxo merkledag)
	dagService := dag.NewDAGService(bserv)
	return &Builder{
		opts:        opts,
		dagService:  dagService,
		directories: make(map[string]*dag.ProtoNode),
	}, nil
}

func (b *Builder) addFileToDirectory(ctx context.Context, dirPath string, fileName string, fileNode *dag.ProtoNode) error {
	dir, exists := b.directories[dirPath]
	if !exists {
		var err error
		dir, err = b.createDirectory(ctx, dirPath)
		if err != nil {
			return err
		}
		b.directories[dirPath] = dir
	}

	// If directory is sharded (HAMT)
	if b.opts.DirSharding {
		// Create new HAMT directory if needed
		hamtDir, err := hamt.NewShard(b.dagService, 256) // Using 256 as default size
		if err != nil {
			return fmt.Errorf("failed to create HAMT directory: %w", err)
		}

		// Set the new file in the HAMT directory
		err = hamtDir.Set(ctx, fileName, fileNode)
		if err != nil {
			return fmt.Errorf("failed to set file in HAMT directory: %w", err)
		}

		// Get the node representation
		newDir, err := hamtDir.Node()
		if err != nil {
			return err
		}

		// Update our directory map
		b.directories[dirPath] = newDir.(*dag.ProtoNode)
	} else {
		// Regular directory
		if err := dir.AddNodeLink(fileName, fileNode); err != nil {
			return err
		}
	}

	// Store updated directory
	if err := b.dagService.Add(ctx, dir); err != nil {
		return err
	}

	return nil
}

// createDirectory creates a new directory node
func (b *Builder) createDirectory(ctx context.Context, dirPath string) (*dag.ProtoNode, error) {
	if b.opts.DirSharding {
		// Create a HAMT-sharded directory
		shard, err := hamt.NewShard(b.dagService, 256) // 256 is the standard HAMT size
		if err != nil {
			return nil, fmt.Errorf("failed to create HAMT directory: %w", err)
		}

		// If we want to preserve time, we can get the directory's actual mtime
		if b.opts.PreserveTime {
			info, err := os.Stat(dirPath)
			if err != nil {
				return nil, fmt.Errorf("failed to stat directory: %w", err)
			}
			// Set metadata on the HAMT node
			node, err := shard.Node()
			if err != nil {
				return nil, err
			}
			protoNode := node.(*dag.ProtoNode)
			// Create UnixFS node with mtime
			fsNode, err := ft.FSNodeFromBytes(protoNode.Data())
			if err != nil {
				return nil, err
			}
			fsNode.SetModTime(info.ModTime())
			newData, err := fsNode.GetBytes()
			if err != nil {
				return nil, err
			}
			protoNode.SetData(newData)
			return protoNode, nil
		}

		// Get the node representation
		node, err := shard.Node()
		if err != nil {
			return nil, err
		}

		return node.(*dag.ProtoNode), nil
	}

	// Create a regular directory node
	dir := dag.NodeWithData(ft.FolderPBData())

	// If preserving time, set the mtime
	if b.opts.PreserveTime {
		info, err := os.Stat(dirPath)
		if err != nil {
			return nil, fmt.Errorf("failed to stat directory: %w", err)
		}

		// Create UnixFS node with mtime
		fsNode, err := ft.FSNodeFromBytes(dir.Data())
		if err != nil {
			return nil, err
		}
		fsNode.SetModTime(info.ModTime())
		newData, err := fsNode.GetBytes()
		if err != nil {
			return nil, err
		}
		dir.SetData(newData)
	}

	// Add the directory to the DAG service
	if err := b.dagService.Add(ctx, dir); err != nil {
		return nil, fmt.Errorf("failed to add directory to DAG service: %w", err)
	}

	return dir, nil
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

// func (b *Builder) AddFile(ctx context.Context, path string) error {
// 	file, err := os.Open(path)
// 	if err != nil {
// 		return fmt.Errorf("failed to open file: %w", err)
// 	}
// 	defer file.Close()

// 	// Get file info for metadata
// 	info, err := file.Stat()
// 	if err != nil {
// 		return fmt.Errorf("failed to stat file: %w", err)
// 	}

// 	// Create a chunker based on options
// 	chunker := b.createChunker(file)

// 	// Create UnixFS DAG params
// 	params := helpers.DagBuilderParams{
// 		Maxlinks:   helpers.DefaultLinksPerBlock,
// 		RawLeaves:  true,
// 		CidBuilder: cid.V1Builder{Codec: cid.DagProtobuf, MhType: b.opts.HashFunc},
// 		Dagserv:    b.dagService,
// 	}

// 	// Create DAG builder
// 	db, err := params.New(chunker)
// 	if err != nil {
// 		return fmt.Errorf("failed to create DAG builder: %w", err)
// 	}

// 	// Build balanced DAG
// 	node, err := balanced.Layout(db)
// 	if err != nil {
// 		return fmt.Errorf("failed to build DAG: %w", err)
// 	}

// 	// Store the root CID
// 	b.root = node.Cid()
// 	b.size = info.Size()

// 	return nil
// }

// File: examples/unixfs-builder/builder/builder.go

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

	// Create UnixFS DAG params
	params := helpers.DagBuilderParams{
		Maxlinks:   helpers.DefaultLinksPerBlock,
		RawLeaves:  true,
		CidBuilder: cid.V1Builder{Codec: cid.DagProtobuf, MhType: b.opts.HashFunc},
		Dagserv:    b.dagService,
	}

	// Create a chunker based on options
	var fsChunker chunker.Splitter
	switch b.opts.Chunker {
	case "rabin":
		fsChunker = chunker.NewRabin(file, uint64(b.opts.ChunkSize))
	default: // "size"
		fsChunker = chunker.NewSizeSplitter(file, b.opts.ChunkSize)
	}

	// Create DAG builder
	db, err := params.New(fsChunker)
	if err != nil {
		return fmt.Errorf("failed to create DAG builder: %w", err)
	}

	// Create UnixFS node with file metadata
	fsNode := ft.NewFSNode(ft.TFile)
	if b.opts.PreserveTime {
		fsNode.SetModTime(info.ModTime())
	}

	// Set mode if requested
	if b.opts.PreserveMode {
		fsNode.SetMode(info.Mode())
	}

	// Build balanced DAG
	node, err := balanced.Layout(db)
	if err != nil {
		return fmt.Errorf("failed to build DAG: %w", err)
	}

	// Store the root CID
	b.root = node.Cid()
	b.size = info.Size()

	// Store the node in the DAG service
	if err := b.dagService.Add(ctx, node); err != nil {
		return fmt.Errorf("failed to add node to DAG service: %w", err)
	}

	return nil
}

// AddDirectory adds a directory and its contents to the UnixFS DAG
func (b *Builder) AddDirectory(ctx context.Context, dirPath string) error {
	// Track the root directory
	rootDir, err := b.createDirectory(ctx, dirPath)
	if err != nil {
		return err
	}
	b.directories[dirPath] = rootDir

	// Walk through the directory
	err = filepath.Walk(dirPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Get relative path from root directory
		relPath, err := filepath.Rel(dirPath, filePath)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}

		if info.IsDir() {
			// Create directory node if it doesn't exist
			if _, exists := b.directories[filePath]; !exists {
				dirNode, err := b.createDirectory(ctx, filePath)
				if err != nil {
					return err
				}
				b.directories[filePath] = dirNode
			}
			return nil
		}

		// Add file and get its node
		if err := b.AddFile(ctx, filePath); err != nil {
			return fmt.Errorf("failed to add file %s: %w", relPath, err)
		}

		// Add file to its parent directory
		parentDir := path.Dir(filePath)
		fileName := path.Base(filePath)
		fileNode, err := b.dagService.Get(ctx, b.root)
		if err != nil {
			return err
		}

		if err := b.addFileToDirectory(ctx, parentDir, fileName, fileNode.(*dag.ProtoNode)); err != nil {
			return fmt.Errorf("failed to add file to directory: %w", err)
		}

		fmt.Printf("Added %s\n", relPath)
		return nil
	})

	if err != nil {
		return err
	}

	// Set root to the root directory
	b.root = b.directories[dirPath].Cid()
	return nil
}
