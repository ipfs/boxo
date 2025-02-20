package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/boxo/examples/unixfs-builder/builder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestData(t *testing.T) string {
	// Create temporary test directory
	tmpDir, err := os.MkdirTemp("", "unixfs-builder-test-*")
	require.NoError(t, err)

	// Create test files
	testFiles := map[string]string{
		"small.txt":      "Hello, IPFS!",
		"medium.txt":     string(make([]byte, 1024)), // 1KB file
		"subdir/a.txt":   "File A",
		"subdir/b.txt":   "File B",
		"subdir/c/d.txt": "Nested file",
	}

	for path, content := range testFiles {
		fullPath := filepath.Join(tmpDir, path)
		err := os.MkdirAll(filepath.Dir(fullPath), 0755)
		require.NoError(t, err)
		err = os.WriteFile(fullPath, []byte(content), 0644)
		require.NoError(t, err)
	}

	return tmpDir
}

func TestSingleFileAdd(t *testing.T) {
	tmpDir := setupTestData(t)
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()
	b, err := builder.NewBuilder(ctx, builder.DefaultOptions())
	require.NoError(t, err)

	// Test adding a single file
	err = b.AddFile(ctx, filepath.Join(tmpDir, "small.txt"))
	assert.NoError(t, err)
	assert.NotEmpty(t, b.Root())
}

func TestDirectoryAdd(t *testing.T) {
	tmpDir := setupTestData(t)
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()
	opts := builder.DefaultOptions()
	opts.PreserveTime = true
	opts.PreserveMode = true

	b, err := builder.NewBuilder(ctx, opts)
	require.NoError(t, err)

	// Test adding a directory
	err = b.AddDirectory(ctx, filepath.Join(tmpDir, "subdir"))
	assert.NoError(t, err)
	assert.NotEmpty(t, b.Root())
}

func TestHAMTSharding(t *testing.T) {
	tmpDir := setupTestData(t)
	defer os.RemoveAll(tmpDir)

	// Create many files to trigger HAMT sharding
	for i := 0; i < 1000; i++ {
		path := filepath.Join(tmpDir, "many", fmt.Sprintf("file%d.txt", i))
		err := os.MkdirAll(filepath.Dir(path), 0755)
		require.NoError(t, err)
		err = os.WriteFile(path, []byte(fmt.Sprintf("content %d", i)), 0644)
		require.NoError(t, err)
	}

	ctx := context.Background()
	opts := builder.DefaultOptions()
	opts.DirSharding = true

	b, err := builder.NewBuilder(ctx, opts)
	require.NoError(t, err)

	// Test adding a large directory
	err = b.AddDirectory(ctx, filepath.Join(tmpDir, "many"))
	assert.NoError(t, err)
	assert.NotEmpty(t, b.Root())
}

func TestProgressReporting(t *testing.T) {
	tmpDir := setupTestData(t)
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()
	b, err := builder.NewBuilder(ctx, builder.DefaultOptions())
	require.NoError(t, err)

	var progressCalled bool
	b.WithProgress(func(p builder.Progress) {
		progressCalled = true
		assert.NotEmpty(t, p.Path)
		assert.NotZero(t, p.TotalBytes)
		assert.Contains(t, []string{"file", "directory", "chunk"}, p.Operation)
	})

	err = b.AddFile(ctx, filepath.Join(tmpDir, "medium.txt"))
	assert.NoError(t, err)
	assert.True(t, progressCalled)
}

func TestErrorHandling(t *testing.T) {
	ctx := context.Background()
	b, err := builder.NewBuilder(ctx, builder.DefaultOptions())
	require.NoError(t, err)

	// Test non-existent file
	err = b.AddFile(ctx, "nonexistent.txt")
	assert.Error(t, err)

	// Test non-existent directory
	err = b.AddDirectory(ctx, "nonexistent-dir")
	assert.Error(t, err)
}
