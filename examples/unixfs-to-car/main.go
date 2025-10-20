package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/ipfs/boxo/blockservice"
	bxstore "github.com/ipfs/boxo/blockstore"
	chunker "github.com/ipfs/boxo/chunker"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/boxo/ipld/unixfs/importer/trickle"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/boxo/mfs"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/multiformats/go-multihash"
)

type config struct {
	inputPath  string
	outputPath string

	// Chunking options
	chunker   string
	rawLeaves bool

	// CID options
	cidVersion  int
	hashFunc    string
	inline      bool
	inlineLimit int

	// Layout options
	trickle bool

	// UnixFS options
	maxDirectoryLinks int

	// Metadata options
	preserveMode  bool
	preserveMtime bool
}

func main() {
	flag.Usage = func() {
		fmt.Println("Usage: unixfs-to-car [flags] <file-or-directory> <output.car>")
		fmt.Println("\nConverts files/directories to UnixFS DAG and exports as CAR")
		fmt.Println("Demonstrates proper chunking, HAMT sharding, and MFS integration")
		fmt.Println("\nFlags (matching ipfs add):")
		flag.PrintDefaults()
	}

	cfg := config{}

	// Chunking flags
	flag.StringVar(&cfg.chunker, "chunker", "size-262144", "Chunking algorithm: size-[bytes], rabin-[min]-[avg]-[max], or buzhash")
	flag.BoolVar(&cfg.rawLeaves, "raw-leaves", false, "Use raw blocks for leaf nodes")

	// CID flags
	flag.IntVar(&cfg.cidVersion, "cid-version", 1, "CID version (0 or 1)")
	flag.StringVar(&cfg.hashFunc, "hash", "sha2-256", "Hash function (sha2-256, blake2b-256, etc)")
	flag.BoolVar(&cfg.inline, "inline", false, "Inline small blocks into CIDs")
	flag.IntVar(&cfg.inlineLimit, "inline-limit", 32, "Maximum block size to inline (max 127 bytes)")

	// Layout flags
	flag.BoolVar(&cfg.trickle, "trickle", false, "Use trickle-dag format for dag generation")

	// UnixFS structure flags (HAMT sharding threshold)
	flag.IntVar(&cfg.maxDirectoryLinks, "max-directory-links", 174, "Maximum links in basic directory before HAMT sharding")

	// Metadata flags
	flag.BoolVar(&cfg.preserveMode, "preserve-mode", false, "Preserve file permissions")
	flag.BoolVar(&cfg.preserveMtime, "preserve-mtime", false, "Preserve modification times")

	flag.Parse()

	if flag.NArg() < 2 {
		flag.Usage()
		os.Exit(1)
	}

	cfg.inputPath = flag.Arg(0)
	cfg.outputPath = flag.Arg(1)

	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg config) error {
	ctx := context.Background()

	// Parse hash function
	hashCode, ok := multihash.Names[cfg.hashFunc]
	if !ok {
		return fmt.Errorf("unknown hash function: %s", cfg.hashFunc)
	}

	// Validate inline limit
	if cfg.inline && cfg.inlineLimit > 127 {
		return fmt.Errorf("inline-limit must be <= 127 bytes")
	}

	// Check if input exists
	info, err := os.Stat(cfg.inputPath)
	if err != nil {
		return err
	}

	// Setup blockstore and DAG service
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bs := bxstore.NewBlockstore(dstore)
	bserv := blockservice.New(bs, offline.Exchange(bs))
	dagService := merkledag.NewDAGService(bserv)

	// Create MFS root
	emptyDir := ft.EmptyDirNode()

	// Set CID builder on empty dir
	prefix := cid.Prefix{
		Version:  uint64(cfg.cidVersion),
		Codec:    cid.DagProtobuf,
		MhType:   hashCode,
		MhLength: -1,
	}
	emptyDir.SetCidBuilder(&prefix)

	root, err := mfs.NewRoot(ctx, dagService, emptyDir, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to create MFS root: %w", err)
	}

	// Import file or directory using UnixFS importer, then add to MFS
	var node format.Node
	if info.IsDir() {
		node, err = importDirectory(ctx, cfg.inputPath, dagService, cfg, hashCode)
	} else {
		node, err = importFile(ctx, cfg.inputPath, dagService, cfg, hashCode)
	}
	if err != nil {
		return err
	}

	// Add the imported node to MFS
	name := filepath.Base(cfg.inputPath)
	err = mfs.PutNode(root, "/"+name, node)
	if err != nil {
		return fmt.Errorf("failed to add to MFS: %w", err)
	}

	// Flush MFS
	err = root.Flush()
	if err != nil {
		return err
	}

	// Get root CID from MFS
	rootDir := root.GetDirectory()
	rootNode, err := rootDir.GetNode()
	if err != nil {
		return err
	}

	rootCid := rootNode.Cid()
	fmt.Printf("Root CID: %s\n", rootCid)

	// Export to CAR
	return exportToCAR(ctx, bs, rootCid, cfg.outputPath)
}

// importFile imports a single file using proper chunking
func importFile(ctx context.Context, filePath string, dagService format.DAGService, cfg config, hashCode uint64) (format.Node, error) {
	// Open file
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Get file info for metadata
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	// Parse chunker
	spl, err := parseChunker(f, cfg.chunker)
	if err != nil {
		return nil, err
	}

	// Setup CID builder
	cidPrefix := &cid.Prefix{
		Version:  uint64(cfg.cidVersion),
		Codec:    cid.DagProtobuf,
		MhType:   hashCode,
		MhLength: -1,
	}

	var cidBuilder cid.Builder = cidPrefix

	// Wrap with inline builder if requested
	if cfg.inline {
		cidBuilder = &inlineBuilder{
			Prefix: cidPrefix,
			Limit:  cfg.inlineLimit,
		}
	}

	// Setup DAG builder params (same as Kubo's ipfs add)
	params := helpers.DagBuilderParams{
		Dagserv:    dagService,
		RawLeaves:  cfg.rawLeaves,
		Maxlinks:   helpers.DefaultLinksPerBlock,
		CidBuilder: cidBuilder,
	}

	db, err := params.New(spl)
	if err != nil {
		return nil, err
	}

	// Build the DAG using selected layout
	var node format.Node
	if cfg.trickle {
		node, err = trickle.Layout(db)
	} else {
		node, err = balanced.Layout(db)
	}
	if err != nil {
		return nil, err
	}

	// Add metadata if requested
	if cfg.preserveMode || cfg.preserveMtime {
		node, err = addMetadata(ctx, node, stat, cfg, dagService)
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

// importDirectory imports a directory recursively with HAMT sharding
func importDirectory(ctx context.Context, dirPath string, dagService format.DAGService, cfg config, hashCode uint64) (format.Node, error) {
	// Setup CID builder
	cidPrefix := &cid.Prefix{
		Version:  uint64(cfg.cidVersion),
		Codec:    cid.DagProtobuf,
		MhType:   hashCode,
		MhLength: -1,
	}

	var cidBuilder cid.Builder = cidPrefix

	// Wrap with inline builder if requested
	if cfg.inline {
		cidBuilder = &inlineBuilder{
			Prefix: cidPrefix,
			Limit:  cfg.inlineLimit,
		}
	}

	// Setup DAG builder params
	params := helpers.DagBuilderParams{
		Dagserv:    dagService,
		RawLeaves:  cfg.rawLeaves,
		Maxlinks:   helpers.DefaultLinksPerBlock,
		CidBuilder: cidBuilder,
	}

	// Recursively add directory
	return addDirectoryRecursive(ctx, dirPath, dagService, params, cfg)
}

// addDirectoryRecursive recursively adds a directory, applying HAMT sharding when needed
func addDirectoryRecursive(ctx context.Context, dirPath string, dagService format.DAGService, params helpers.DagBuilderParams, cfg config) (format.Node, error) {
	// Read directory entries
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	// Create a new UnixFS directory or HAMT directory based on size
	var dir uio.Directory
	if len(entries) > cfg.maxDirectoryLinks {
		// Use HAMT for large directories (with default fanout 256)
		dir, err = uio.NewHAMTDirectory(dagService, 256)
		if err != nil {
			return nil, err
		}
	} else {
		// Use basic directory for small directories
		dir, err = uio.NewDirectory(dagService)
		if err != nil {
			return nil, err
		}
	}

	// Add each entry
	for _, entry := range entries {
		entryPath := filepath.Join(dirPath, entry.Name())
		entryInfo, err := entry.Info()
		if err != nil {
			return nil, err
		}

		var childNode format.Node

		if entry.IsDir() {
			// Recursively add subdirectory
			childNode, err = addDirectoryRecursive(ctx, entryPath, dagService, params, cfg)
			if err != nil {
				return nil, err
			}
		} else {
			// Get hash code from CID builder
			var hashCode uint64
			if inlineB, ok := params.CidBuilder.(*inlineBuilder); ok {
				hashCode = inlineB.Prefix.MhType
			} else {
				hashCode = params.CidBuilder.(*cid.Prefix).MhType
			}
			
			// Add file with chunking
			childNode, err = importFile(ctx, entryPath, dagService, cfg, hashCode)
			if err != nil {
				return nil, err
			}
		}

		// Add metadata if requested
		if cfg.preserveMode || cfg.preserveMtime {
			childNode, err = addMetadata(ctx, childNode, entryInfo, cfg, dagService)
			if err != nil {
				return nil, err
			}
		}

		// Add link to directory
		err = dir.AddChild(ctx, entry.Name(), childNode)
		if err != nil {
			return nil, err
		}
	}

	// Get the directory node
	dirNode, err := dir.GetNode()
	if err != nil {
		return nil, err
	}

	// Explicitly add directory node to DAG service
	err = dagService.Add(ctx, dirNode)
	if err != nil {
		return nil, err
	}

	return dirNode, nil
}

// parseChunker creates a chunker from the config string
func parseChunker(r io.Reader, chunkerStr string) (chunker.Splitter, error) {
	parts := strings.Split(chunkerStr, "-")

	switch parts[0] {
	case "size":
		if len(parts) != 2 {
			return nil, errors.New("size chunker expects format: size-[bytes]")
		}
		var size int64
		_, err := fmt.Sscanf(parts[1], "%d", &size)
		if err != nil {
			return nil, fmt.Errorf("invalid size: %w", err)
		}
		return chunker.NewSizeSplitter(r, size), nil

	case "rabin":
		if len(parts) != 4 {
			return nil, errors.New("rabin chunker expects format: rabin-[min]-[avg]-[max]")
		}
		var avg uint64
		_, err := fmt.Sscanf(parts[2], "%d", &avg)
		if err != nil {
			return nil, fmt.Errorf("invalid rabin avg: %w", err)
		}
		return chunker.NewRabin(r, avg), nil

	case "buzhash":
		return chunker.NewBuzhash(r), nil

	default:
		return nil, fmt.Errorf("unknown chunker type: %s (supported: size, rabin, buzhash)", parts[0])
	}
}

// addMetadata adds mode/mtime metadata to a node
func addMetadata(ctx context.Context, node format.Node, stat os.FileInfo, cfg config, dagService format.DAGService) (format.Node, error) {
	protoNode, ok := node.(*merkledag.ProtoNode)
	if !ok {
		// Can't add metadata to non-proto node
		return node, nil
	}

	fsn, err := ft.FSNodeFromBytes(protoNode.Data())
	if err != nil {
		return node, nil
	}

	// Update metadata
	if cfg.preserveMode {
		fsn.SetMode(stat.Mode())
	}
	if cfg.preserveMtime {
		fsn.SetModTime(stat.ModTime())
	}

	data, err := fsn.GetBytes()
	if err != nil {
		return nil, err
	}

	newNode := merkledag.NodeWithData(data)
	for _, link := range protoNode.Links() {
		err = newNode.AddRawLink(link.Name, link)
		if err != nil {
			return nil, err
		}
	}

	err = dagService.Add(ctx, newNode)
	if err != nil {
		return nil, err
	}

	return newNode, nil
}

// inlineBuilder wraps a CID builder to support inlining small blocks
type inlineBuilder struct {
	Prefix *cid.Prefix
	Limit  int
}

func (ib *inlineBuilder) Sum(data []byte) (cid.Cid, error) {
	if len(data) <= ib.Limit {
		// Use identity hash for small blocks
		mh, err := multihash.Sum(data, multihash.IDENTITY, -1)
		if err != nil {
			return cid.Undef, err
		}
		return cid.NewCidV1(ib.Prefix.Codec, mh), nil
	}
	return ib.Prefix.Sum(data)
}

func (ib *inlineBuilder) GetCodec() uint64 {
	return ib.Prefix.Codec
}

func (ib *inlineBuilder) WithCodec(codec uint64) cid.Builder {
	newPrefix := *ib.Prefix
	newPrefix.Codec = codec
	return &inlineBuilder{
		Prefix: &newPrefix,
		Limit:  ib.Limit,
	}
}

func exportToCAR(ctx context.Context, bs bxstore.Blockstore, rootCid cid.Cid, outputPath string) error {
	// Create a ReadWrite CAR
	rw, err := blockstore.OpenReadWrite(outputPath, []cid.Cid{rootCid})
	if err != nil {
		return fmt.Errorf("failed to create CAR: %w", err)
	}
	defer rw.Finalize()

	// Copy all blocks from source blockstore to CAR
	return copyBlocks(ctx, bs, rw, rootCid)
}

func copyBlocks(ctx context.Context, src bxstore.Blockstore, dst *blockstore.ReadWrite, root cid.Cid) error {
	// Get the root block
	blk, err := src.Get(ctx, root)
	if err != nil {
		return err
	}

	// Put in destination
	err = dst.Put(ctx, blk)
	if err != nil {
		return err
	}

	// Decode and recursively copy linked blocks
	node, err := merkledag.DecodeProtobuf(blk.RawData())
	if err != nil {
		// Not a dag-pb node, we're done
		return nil
	}

	// Copy all linked blocks
	for _, link := range node.Links() {
		if err := copyBlocks(ctx, src, dst, link.Cid); err != nil {
			return err
		}
	}

	return nil
}