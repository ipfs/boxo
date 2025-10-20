package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/ipfs/boxo/blockservice"
	bxstore "github.com/ipfs/boxo/blockstore"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/mfs"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/multiformats/go-multihash"
)

func main() {
	flag.Usage = func() {
		fmt.Println("Usage: unixfs-to-car [flags] <file> <output.car>")
		fmt.Println("\nConverts a file to UnixFS DAG using MFS and exports as CAR")
		fmt.Println("MFS (Mutable File System) is the recommended high-level API for working with UnixFS")
		flag.PrintDefaults()
	}

	hashFunc := flag.String("hash", "sha2-256", "hash function (sha2-256, blake2b-256, etc)")
	flag.Parse()

	if flag.NArg() < 2 {
		flag.Usage()
		os.Exit(1)
	}

	if err := run(flag.Arg(0), flag.Arg(1), *hashFunc); err != nil {
		log.Fatal(err)
	}
}

func run(inputPath, outputPath string, hashFunc string) error {
	ctx := context.Background()

	// Parse hash function
	hashCode, ok := multihash.Names[hashFunc]
	if !ok {
		return fmt.Errorf("unknown hash function: %s", hashFunc)
	}

	// Check if input is a file
	info, err := os.Stat(inputPath)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("directory support not yet implemented")
	}

	// Setup blockstore and DAG service
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bs := bxstore.NewBlockstore(dstore)
	bserv := blockservice.New(bs, offline.Exchange(bs))
	dagService := merkledag.NewDAGService(bserv)

	// Create MFS root - this is the recommended approach
	emptyDir := ft.EmptyDirNode()
	root, err := mfs.NewRoot(ctx, dagService, emptyDir, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to create MFS root: %w", err)
	}

	// Add file using MFS
	rootCid, err := addFileToMFS(ctx, root, inputPath, dagService, hashCode)
	if err != nil {
		return err
	}

	fmt.Printf("Root CID: %s\n", rootCid)

	// Export to CAR
	return exportToCAR(ctx, bs, rootCid, outputPath)
}

// addFileToMFS demonstrates adding a file using MFS (recommended approach)
func addFileToMFS(ctx context.Context, root *mfs.Root, filePath string, dagService format.DAGService, hashCode uint64) (cid.Cid, error) {
	// Open the source file
	srcFile, err := os.Open(filePath)
	if err != nil {
		return cid.Undef, err
	}
	defer srcFile.Close()

	// Import file to create a DAG node
	// Note: This follows the pattern used by 'ipfs add --to-files'
	node, err := importToDAG(ctx, srcFile, dagService, hashCode)
	if err != nil {
		return cid.Undef, err
	}

	// Add the node to MFS using PutNode (recommended MFS operation)
	fileName := "file"
	err = mfs.PutNode(root, "/"+fileName, node)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to add to MFS: %w", err)
	}

	// Flush MFS to finalize
	err = root.Flush()
	if err != nil {
		return cid.Undef, err
	}

	// Get the root CID from MFS
	rootDir := root.GetDirectory()
	rootNode, err := rootDir.GetNode()
	if err != nil {
		return cid.Undef, err
	}

	return rootNode.Cid(), nil
}

// importToDAG creates a UnixFS DAG node from a file
// This helper is needed because MFS operates on DAG nodes
func importToDAG(ctx context.Context, file io.Reader, dagService format.DAGService, hashCode uint64) (format.Node, error) {
	// Read file data
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	// Create a UnixFS file node with the data
	node := merkledag.NodeWithData(ft.FilePBData(data, uint64(len(data))))

	// Set the CID builder with the specified hash function
	prefix := cid.Prefix{
		Version:  1,
		Codec:    cid.DagProtobuf,
		MhType:   hashCode,
		MhLength: -1,
	}
	node.SetCidBuilder(&prefix)

	// Add to DAG service
	err = dagService.Add(ctx, node)
	if err != nil {
		return nil, err
	}

	return node, nil
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