# UnixFS to CAR Example

This example demonstrates how to convert a local file into a UnixFS DAG and export it as a [CAR (Content Addressable aRchive)](https://ipld.io/specs/transport/car/) file, using **MFS (Mutable File System)** - the recommended high-level API for working with UnixFS structures.

## Why This Example?

Developers often need to programmatically create UnixFS structures (similar to `ipfs add`) but are confused about which APIs to use. This example shows the **recommended approach** using MFS.

## Why MFS?

MFS is the recommended API because:
- **High-level abstraction** - automatically handles DAG structure and links
- **Production-tested** - powers Kubo's `ipfs files` commands
- **Simpler** - no need to manually manage DAG building, chunking, and node creation
- **Consistent** - same patterns used throughout Kubo

This example follows the same pattern as `ipfs add --to-files`.

## Build
```bash
cd examples/unixfs-to-car
go build -o unixfs-to-car
```

## Usage

### Basic Usage

Convert a file to CAR:
```bash
echo "Hello, IPFS!" > hello.txt
./unixfs-to-car hello.txt output.car
```

Output:
```
Root CID: bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi
```

### With Different Hash Function
```bash
./unixfs-to-car -hash blake2b-256 myfile.txt output.car
```

### Verify the CAR File

You can inspect the generated CAR file using:
```bash
ipfs dag import output.car
ipfs cat <Root-CID>
```

## How It Works

The example demonstrates three key steps:

### 1. Create MFS Root
```go
emptyDir := ft.EmptyDirNode()
root, _ := mfs.NewRoot(ctx, dagService, emptyDir, nil, nil)
```

MFS requires a root directory to work with. This creates an empty MFS filesystem.

### 2. Add File to MFS
```go
// Import file to create node
node, _ := importToDAG(ctx, file, dagService)

// Add to MFS using PutNode (recommended MFS operation)
mfs.PutNode(root, "/filename", node)

// Flush to finalize
root.Flush()
```

This follows the same pattern as `ipfs add --to-files /path`:
1. Create a DAG node from the file
2. Use `mfs.PutNode()` to add it to MFS
3. Flush to ensure changes are persisted

### 3. Export to CAR
```go
rootDir := root.GetDirectory()
rootNode, _ := rootDir.GetNode()
carv1.WriteCar(ctx, dagService, []cid.Cid{rootNode.Cid()}, outputFile)
```

Get the root CID from MFS and export all blocks to a CAR file.

## Code Structure
```
main.go
├── run()              # Main logic
├── addFileToMFS()     # Demonstrates MFS file addition
├── importToDAG()      # Helper to create DAG node
└── exportToCAR()      # CAR export
```

## Current Implementation

This example uses `mfs.PutNode()` which requires a pre-created node. This matches how Kubo's `ipfs add --to-files` works internally (see `kubo/core/commands/add.go`).

**Note**: For large files, a more complete implementation would use chunking and the UnixFS importer before calling `mfs.PutNode()`. The current version handles small-to-medium files as a clear demonstration of the MFS pattern.

## Future Enhancements

Potential improvements for this example:
- [ ] Directory support with recursive addition
- [ ] Chunking for large files
- [ ] HAMT sharding for large directories
- [ ] Metadata preservation (mtime, mode)
- [ ] Progress reporting

These enhancements would demonstrate more MFS features while keeping the core pattern clear.

## Questions for Maintainers

1. **Direct MFS File Writing**: Is there a recommended pattern for writing directly to MFS files using file descriptors (instead of creating a node first)? This would make the example even more MFS-centric.

2. **Chunking in MFS Context**: For large files, should we use the UnixFS importer before `mfs.PutNode()`, or is there an MFS-native chunking approach?

3. **Example Scope**: Is this level of simplicity appropriate, or should we include the chunking/large file handling from the start?

## Related Examples

- [car-file-fetcher](../car-file-fetcher/) - Shows how to read CAR files
- [gateway](../gateway/) - Demonstrates serving UnixFS content

## References

- [MFS Documentation](https://pkg.go.dev/github.com/ipfs/boxo/mfs)
- [CAR Specification](https://ipld.io/specs/transport/car/)
- [UnixFS Specification](https://github.com/ipfs/specs/blob/main/UNIXFS.md)
- [Kubo's add.go](https://github.com/ipfs/kubo/blob/master/core/commands/add.go) - Reference implementation