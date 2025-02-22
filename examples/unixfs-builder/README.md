First, let's update the README.md:

```markdown
# UnixFS Builder Example

This example demonstrates how to programmatically create UnixFS data structures that are compatible with IPFS, similar to `ipfs add` but with more control over the process.

## Features

- Add single files with custom chunking and hashing
- Create directories with multiple files
- Handle large directories using HAMT sharding
- Preserve file metadata (mode, mtime)
- Progress reporting with human-readable sizes
- Support for different chunking strategies (size-based, rabin)

## Installation and Setup

1. Clone the repository:
```bash
git clone https://github.com/ipfs/boxo.git
cd boxo/examples
```

2. Create the unixfs-builder directory:
```bash
mkdir unixfs-builder
cd unixfs-builder
```

3. Generate test data:
```bash
mkdir -p testdata
cd testdata
go run generate.go
cd ..
```

## Running the Example

The builder supports various options through command-line flags:

```bash
# Add a single file
go run . add myfile.txt

# Add a directory recursively
go run . add -r mydirectory/

# Add with custom chunk size (256KiB)
go run . add --chunk-size 262144 largefile.txt

# Preserve file timestamps
go run . add --preserve-time photo.jpg

# Enable HAMT sharding for large directories
go run . add -r --sharding large-directory/
```

## Example Use Cases

1. Basic file addition:
```bash
echo "Hello IPFS" > test.txt
go run . add test.txt
```

2. Directory with metadata preservation:
```bash
go run . add -r --preserve-time --preserve-mode ./mydirectory
```

3. Large file with custom chunking:
```bash
go run . add --chunk-size 1048576 --chunker rabin bigfile.zip
```

## Project Structure
```
unixfs-builder/
├── main.go              # Main entry point
├── main_test.go         # Integration tests
├── builder/
│   ├── options.go       # Builder configuration
│   ├── builder.go       # Core UnixFS building logic
│   └── progress.go      # Progress reporting
├── examples/
│   ├── single_file.go   # Single file example
│   ├── directory.go     # Directory example
│   └── large_directory.go # HAMT sharding example
└── testdata/           # Test files
```

## Testing

Run the tests with:
```bash
go test ./...
```

## Error Handling

The builder provides detailed error messages. Common errors include:
- File not found
- Permission denied
- Invalid chunking parameters
- Out of memory when handling large files