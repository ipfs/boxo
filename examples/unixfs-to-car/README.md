# UnixFS to CAR Example

This example demonstrates how to convert local files and directories into UnixFS DAGs and export them as [CAR (Content Addressable aRchive)](https://ipld.io/specs/transport/car/) files, using **MFS (Mutable File System)** and the UnixFS importer - the same tools that power `ipfs add`.

This is a **complete, production-ready example** showing:
- ✅ Proper chunking (not reading entire files into memory)
- ✅ Directory support with recursive addition
- ✅ HAMT sharding for large directories
- ✅ All major `ipfs add` flags
- ✅ Metadata preservation (mode/mtime)

## Why This Example?

The original issue (#663) requested:
> "an example of `ipfs add + ipfs dag export` equivalent for reading a file or a directory from local filesystem, and turning it into a CAR with UnixFS DAG that was properly chunked / HAMT-sharded"

This example fulfills that requirement using the **recommended MFS approach** as requested by maintainers in PR #851.

## Build
```bash
cd examples/unixfs-to-car
go build -o unixfs-to-car
```

## Basic Usage

### Single File
```bash
echo "Hello, IPFS!" > hello.txt
./unixfs-to-car hello.txt output.car
```

Output:
```
Root CID: bafybeig...
```

### Directory
```bash
mkdir mydir
echo "File 1" > mydir/file1.txt
echo "File 2" > mydir/file2.txt
./unixfs-to-car mydir output.car
```

### Large File with Chunking
```bash
# Create a 10MB file
dd if=/dev/urandom of=large.bin bs=1M count=10

# Convert with 1MB chunks
./unixfs-to-car --chunker size-1048576 large.bin output.car
```

## All Supported Flags

### Chunking Options
```bash
# Size-based chunking (default: 256KB)
./unixfs-to-car --chunker size-262144 file.txt output.car

# Rabin fingerprint chunking (content-defined)
./unixfs-to-car --chunker rabin-262144-524288-1048576 file.txt output.car

# Buzhash chunking
./unixfs-to-car --chunker buzhash file.txt output.car

# Use raw leaves (recommended for deduplication)
./unixfs-to-car --raw-leaves file.txt output.car
```

### CID Options
```bash
# CID version 0 (default: 1)
./unixfs-to-car --cid-version 0 file.txt output.car

# Different hash functions
./unixfs-to-car --hash sha2-512 file.txt output.car
./unixfs-to-car --hash blake2b-256 file.txt output.car

# Inline small blocks into CIDs
./unixfs-to-car --inline --inline-limit 32 file.txt output.car
```

### Layout Options
```bash
# Use trickle DAG instead of balanced (better for streaming)
./unixfs-to-car --trickle file.txt output.car
```

### Directory Options
```bash
# Control HAMT sharding threshold (default: 174 links)
./unixfs-to-car --max-directory-links 100 mydir output.car
```

### Metadata Options
```bash
# Preserve file permissions and modification times
./unixfs-to-car --preserve-mode --preserve-mtime mydir output.car
```

### Combined Example
```bash
# Production-ready command with all best practices
./unixfs-to-car \
  --chunker size-1048576 \
  --raw-leaves \
  --cid-version 1 \
  --hash sha2-256 \
  --preserve-mode \
  --preserve-mtime \
  mydir output.car
```

## How It Works

This example demonstrates the **recommended workflow** for creating UnixFS structures:

### 1. File Import with Proper Chunking
```go
// Parse user's chunker preference (size, rabin, or buzhash)
spl, err := parseChunker(file, cfg.chunker)

// Setup DAG builder with chunker
params := helpers.DagBuilderParams{
    Dagserv:    dagService,
    RawLeaves:  cfg.rawLeaves,
    CidBuilder: cidBuilder,
}
db, err := params.New(spl)

// Build DAG using balanced or trickle layout
node, err := balanced.Layout(db)
```

**Key Point**: Files are **streamed and chunked**, not read entirely into memory. This allows processing of arbitrarily large files.

### 2. Directory Import with HAMT Sharding
```go
// For small directories (< 174 files)
dir := uio.NewDirectory(dagService)

// For large directories (automatic HAMT sharding)
if len(entries) > cfg.maxDirectoryLinks {
    dir, err = uio.NewHAMTDirectory(ctx, dagService)
}

// Recursively add all entries
for _, entry := range entries {
    dir.AddChild(ctx, entry.Name(), childNode)
}
```

**Key Point**: Large directories automatically use HAMT sharding, allowing millions of entries without performance degradation.

### 3. MFS Integration
```go
// Create MFS root
root, err := mfs.NewRoot(ctx, dagService, emptyDir, nil, nil)

// Add imported structure to MFS
mfs.PutNode(root, "/filename", node)

// Flush to persist
root.Flush()

// Get final CID
rootNode, err := root.GetDirectory().GetNode()
```

**Key Point**: MFS provides a high-level API for managing the DAG structure, recommended by Boxo maintainers.

### 4. CAR Export
```go
// Export entire DAG to CAR file
rw, err := blockstore.OpenReadWrite(outputPath, []cid.Cid{rootCid})
copyBlocks(ctx, blockstore, rw, rootCid)
rw.Finalize()
```

## Architecture
```
┌─────────────────┐
│  Local Files    │
│  & Directories  │
└────────┬────────┘
         │
         ▼
┌─────────────────────┐
│  UnixFS Importer    │  ← Chunking, Layout (balanced/trickle)
│  (helpers, chunker) │
└────────┬────────────┘
         │
         ▼
┌─────────────────┐
│       MFS       │  ← High-level API (PutNode, Flush)
│  (Recommended)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   DAG Service   │  ← Block storage
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   CAR Export    │  ← Final output
└─────────────────┘
```

## Features Demonstrated

### ✅ Chunking Strategies

- **Size-based**: Fixed-size chunks (fast, predictable)
- **Rabin**: Content-defined chunks (better deduplication)
- **Buzhash**: Alternative content-defined chunking

### ✅ DAG Layouts

- **Balanced**: Better for random access
- **Trickle**: Better for sequential/streaming access

### ✅ HAMT Sharding

Automatically triggered when directories exceed the threshold:
- **Small directories** (< 174 files): Basic directory
- **Large directories** (≥ 174 files): HAMT-sharded directory

### ✅ Metadata Preservation

- File permissions (Unix mode)
- Modification times (mtime)

### ✅ CID Flexibility

- CID v0 or v1
- Multiple hash functions (SHA2-256, SHA2-512, Blake2b-256, etc.)
- Inline CIDs for tiny blocks

## Testing

Run the comprehensive test suite:
```bash
go test -v
```

Tests cover:
- ✅ Single file conversion
- ✅ Large file chunking
- ✅ Directory conversion (with subdirectories)
- ✅ Raw leaves
- ✅ Trickle DAG
- ✅ Metadata preservation
- ✅ Different hash functions
- ✅ Different chunkers
- ✅ HAMT sharding (200+ files)
- ✅ Error cases

## Verification with IPFS

You can verify the generated CARs with IPFS:
```bash
# Import CAR
ipfs dag import output.car

# Verify content
ipfs cat <Root-CID>
```

## Performance Characteristics

- **Memory**: Constant memory usage regardless of file size (streaming)
- **Chunking**: ~256KB chunks by default (configurable)
- **HAMT**: Scales to millions of directory entries
- **CAR Export**: Efficient block traversal

## Comparison to `ipfs add`

This example replicates the core functionality of `ipfs add`:

| Feature | `ipfs add` | This Example |
|---------|-----------|--------------|
| Chunking | ✅ | ✅ |
| Raw leaves | ✅ | ✅ |
| CID version | ✅ | ✅ |
| Hash functions | ✅ | ✅ |
| Trickle DAG | ✅ | ✅ |
| Directory recursion | ✅ | ✅ |
| HAMT sharding | ✅ | ✅ |
| Metadata preservation | ✅ | ✅ |
| CAR export | Via `dag export` | ✅ Built-in |

## Limitations & Future Work

Current implementation handles the most common use cases. Potential enhancements:

- [ ] Symlink support
- [ ] Filestore integration (--nocopy)
- [ ] Progress reporting
- [ ] Parallel processing for large directories
- [ ] CAR v1 output option

These are intentionally omitted to keep the example focused and understandable.

## Code Structure
```
main.go
├── main()                      # CLI flag parsing
├── run()                       # Main logic
├── importFile()                # File import with chunking
├── importDirectory()           # Directory import with HAMT
├── addDirectoryRecursive()     # Recursive directory traversal
├── parseChunker()              # Chunker configuration
├── addMetadata()               # Mode/mtime preservation
├── inlineBuilder               # Inline CID support
├── exportToCAR()               # CAR export
└── copyBlocks()                # Recursive block copying
```

## Related Examples

- [car-file-fetcher](../car-file-fetcher/) - Reading CAR files
- [gateway](../gateway/) - Serving UnixFS content via HTTP

## References

- [MFS Documentation](https://pkg.go.dev/github.com/ipfs/boxo/mfs)
- [UnixFS Specification](https://github.com/ipfs/specs/blob/main/UNIXFS.md)
- [CAR Specification](https://ipld.io/specs/transport/car/)
- [Kubo's `ipfs add` implementation](https://github.com/ipfs/kubo/blob/master/core/commands/add.go)
- [HAMT Specification](https://github.com/ipfs/specs/blob/main/UNIXFS.md#hamt-sharded-directories)

## License

This example is part of Boxo and follows the same license.