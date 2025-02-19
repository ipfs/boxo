# UnixFS Builder Example

This example demonstrates how to programmatically create UnixFS data structures that are compatible with IPFS. It shows how to:

- Add single files to IPFS with custom chunking and hashing
- Create directories with multiple files
- Handle large directories using HAMT sharding
- Preserve file metadata (mode, mtime)
- Export the resulting data as a CAR file

## Usage

```bash
# Add a single file
go run . add myfile.txt

# Add a directory
go run . add -r mydirectory/

# Customize chunking (like ipfs add --chunker size-262144)
go run . add --chunker size-262144 largefile.zip

# Preserve file times (like ipfs add --nocopy)
go run . add --preserve-time photo.jpg
```

## How It Works

This example shows the lower-level building blocks that power commands like `ipfs add`. It demonstrates:

1. File chunking - splitting large files into smaller blocks
2. Content addressing - generating CIDs for your data
3. UnixFS formatting - creating IPFS-compatible file structures
4. DAG building - connecting blocks into a Merkle-DAG
5. CAR export - saving the data in a portable format