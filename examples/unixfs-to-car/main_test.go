package main

import (
	"os"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	carv2 "github.com/ipld/go-car/v2"
)

func TestBasicFileConversion(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test file
	testFile := filepath.Join(tmpDir, "test.txt")
	testContent := []byte("Hello, IPFS! This is a test file.")
	if err := os.WriteFile(testFile, testContent, 0644); err != nil {
		t.Fatal(err)
	}

	// Run conversion
	outputCAR := filepath.Join(tmpDir, "output.car")
	cfg := config{
		inputPath:  testFile,
		outputPath: outputCAR,
		chunker:    "size-262144",
		cidVersion: 1,
		hashFunc:   "sha2-256",
		rawLeaves:  false,
	}

	if err := run(cfg); err != nil {
		t.Fatal(err)
	}

	// Verify CAR file exists
	if _, err := os.Stat(outputCAR); err != nil {
		t.Fatal("CAR file not created:", err)
	}

	// Verify CAR is valid
	f, err := os.Open(outputCAR)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	cr, err := carv2.NewBlockReader(f)
	if err != nil {
		t.Fatal("Invalid CAR file:", err)
	}

	if len(cr.Roots) == 0 {
		t.Fatal("CAR file has no roots")
	}

	t.Logf("Successfully created CAR with root: %s", cr.Roots[0])
}

func TestLargeFileWithChunking(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a large test file (1MB)
	testFile := filepath.Join(tmpDir, "large.txt")
	largeContent := strings.Repeat("This is a test line.\n", 50000) // ~1MB
	if err := os.WriteFile(testFile, []byte(largeContent), 0644); err != nil {
		t.Fatal(err)
	}

	// Run conversion with small chunk size to ensure chunking
	outputCAR := filepath.Join(tmpDir, "output.car")
	cfg := config{
		inputPath:  testFile,
		outputPath: outputCAR,
		chunker:    "size-32768", // 32KB chunks
		cidVersion: 1,
		hashFunc:   "sha2-256",
		rawLeaves:  true,
	}

	if err := run(cfg); err != nil {
		t.Fatal(err)
	}

	// Verify CAR file exists and has reasonable size
	info, err := os.Stat(outputCAR)
	if err != nil {
		t.Fatal("CAR file not created:", err)
	}

	if info.Size() < 1000 {
		t.Fatal("CAR file suspiciously small, chunking may not be working")
	}

	t.Logf("Large file chunked successfully, CAR size: %d bytes", info.Size())
}

func TestDirectoryConversion(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test directory structure
	testDir := filepath.Join(tmpDir, "testdir")
	if err := os.Mkdir(testDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create files in directory
	files := map[string]string{
		"file1.txt": "Content of file 1",
		"file2.txt": "Content of file 2",
		"file3.txt": "Content of file 3",
	}

	for name, content := range files {
		path := filepath.Join(testDir, name)
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}

	// Create subdirectory
	subDir := filepath.Join(testDir, "subdir")
	if err := os.Mkdir(subDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subDir, "nested.txt"), []byte("Nested content"), 0644); err != nil {
		t.Fatal(err)
	}

	// Run conversion
	outputCAR := filepath.Join(tmpDir, "output.car")
	cfg := config{
		inputPath:         testDir,
		outputPath:        outputCAR,
		chunker:           "size-262144",
		cidVersion:        1,
		hashFunc:          "sha2-256",
		rawLeaves:         false,
		maxDirectoryLinks: 174,
	}

	if err := run(cfg); err != nil {
		t.Fatal(err)
	}

	// Verify CAR file exists
	if _, err := os.Stat(outputCAR); err != nil {
		t.Fatal("CAR file not created:", err)
	}

	t.Log("Directory converted successfully")
}

func TestRawLeaves(t *testing.T) {
	tmpDir := t.TempDir()

	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test content"), 0644); err != nil {
		t.Fatal(err)
	}

	outputCAR := filepath.Join(tmpDir, "output.car")
	cfg := config{
		inputPath:  testFile,
		outputPath: outputCAR,
		chunker:    "size-262144",
		cidVersion: 1,
		hashFunc:   "sha2-256",
		rawLeaves:  true, // Enable raw leaves
	}

	if err := run(cfg); err != nil {
		t.Fatal(err)
	}

	// Verify CAR was created
	if _, err := os.Stat(outputCAR); err != nil {
		t.Fatal("CAR file not created:", err)
	}

	t.Log("Raw leaves test passed")
}

func TestTrickleDAG(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a file that will be chunked
	testFile := filepath.Join(tmpDir, "test.txt")
	content := strings.Repeat("Test data for trickle DAG.\n", 10000)
	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	outputCAR := filepath.Join(tmpDir, "output.car")
	cfg := config{
		inputPath:  testFile,
		outputPath: outputCAR,
		chunker:    "size-32768",
		cidVersion: 1,
		hashFunc:   "sha2-256",
		rawLeaves:  true,
		trickle:    true, // Use trickle DAG
	}

	if err := run(cfg); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(outputCAR); err != nil {
		t.Fatal("CAR file not created:", err)
	}

	t.Log("Trickle DAG test passed")
}

func TestMetadataPreservation(t *testing.T) {
	tmpDir := t.TempDir()

	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test content"), 0644); err != nil {
		t.Fatal(err)
	}

	outputCAR := filepath.Join(tmpDir, "output.car")
	cfg := config{
		inputPath:     testFile,
		outputPath:    outputCAR,
		chunker:       "size-262144",
		cidVersion:    1,
		hashFunc:      "sha2-256",
		preserveMode:  true,
		preserveMtime: true,
	}

	if err := run(cfg); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(outputCAR); err != nil {
		t.Fatal("CAR file not created:", err)
	}

	t.Log("Metadata preservation test passed")
}

func TestDifferentHashFunctions(t *testing.T) {
	testCases := []struct {
		name     string
		hashFunc string
	}{
		{"SHA2-256", "sha2-256"},
		{"SHA2-512", "sha2-512"},
		{"Blake2b-256", "blake2b-256"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()

			testFile := filepath.Join(tmpDir, "test.txt")
			if err := os.WriteFile(testFile, []byte("test content"), 0644); err != nil {
				t.Fatal(err)
			}

			outputCAR := filepath.Join(tmpDir, "output.car")
			cfg := config{
				inputPath:  testFile,
				outputPath: outputCAR,
				chunker:    "size-262144",
				cidVersion: 1,
				hashFunc:   tc.hashFunc,
			}

			if err := run(cfg); err != nil {
				t.Fatalf("Failed with hash %s: %v", tc.hashFunc, err)
			}

			if _, err := os.Stat(outputCAR); err != nil {
				t.Fatal("CAR file not created:", err)
			}
		})
	}
}

func TestDifferentChunkers(t *testing.T) {
	testCases := []struct {
		name    string
		chunker string
	}{
		{"Size-64KB", "size-65536"},
		{"Size-256KB", "size-262144"},
		{"Rabin", "rabin-32768-65536-131072"},
		{"Buzhash", "buzhash"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()

			testFile := filepath.Join(tmpDir, "test.txt")
			content := strings.Repeat("Test data.\n", 10000)
			if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
				t.Fatal(err)
			}

			outputCAR := filepath.Join(tmpDir, "output.car")
			cfg := config{
				inputPath:  testFile,
				outputPath: outputCAR,
				chunker:    tc.chunker,
				cidVersion: 1,
				hashFunc:   "sha2-256",
				rawLeaves:  true,
			}

			if err := run(cfg); err != nil {
				t.Fatalf("Failed with chunker %s: %v", tc.chunker, err)
			}

			if _, err := os.Stat(outputCAR); err != nil {
				t.Fatal("CAR file not created:", err)
			}
		})
	}
}

func TestInvalidHashFunction(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	outputCAR := filepath.Join(tmpDir, "output.car")
	cfg := config{
		inputPath:  testFile,
		outputPath: outputCAR,
		chunker:    "size-262144",
		cidVersion: 1,
		hashFunc:   "invalid-hash-function",
	}

	err := run(cfg)
	if err == nil {
		t.Fatal("Expected error for invalid hash function")
	}

	if !strings.Contains(err.Error(), "unknown hash function") {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestInvalidChunker(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	outputCAR := filepath.Join(tmpDir, "output.car")
	cfg := config{
		inputPath:  testFile,
		outputPath: outputCAR,
		chunker:    "invalid-chunker",
		cidVersion: 1,
		hashFunc:   "sha2-256",
	}

	err := run(cfg)
	if err == nil {
		t.Fatal("Expected error for invalid chunker")
	}

	if !strings.Contains(err.Error(), "unknown chunker type") {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestNonExistentFile(t *testing.T) {
	tmpDir := t.TempDir()

	outputCAR := filepath.Join(tmpDir, "output.car")
	cfg := config{
		inputPath:  "/nonexistent/file.txt",
		outputPath: outputCAR,
		chunker:    "size-262144",
		cidVersion: 1,
		hashFunc:   "sha2-256",
	}

	err := run(cfg)
	if err == nil {
		t.Fatal("Expected error for non-existent file")
	}
}

func TestLargeDirectoryWithHAMT(t *testing.T) {
	tmpDir := t.TempDir()

	// Create directory with many files to trigger HAMT
	testDir := filepath.Join(tmpDir, "large_dir")
	if err := os.Mkdir(testDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create 200 files to exceed HAMT threshold (174)
	for i := 0; i < 200; i++ {
		filename := filepath.Join(testDir, fmt.Sprintf("file%03d.txt", i))
		content := fmt.Sprintf("Content of file %d", i)
		if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}

	outputCAR := filepath.Join(tmpDir, "output.car")
	cfg := config{
		inputPath:         testDir,
		outputPath:        outputCAR,
		chunker:           "size-262144",
		cidVersion:        1,
		hashFunc:          "sha2-256",
		maxDirectoryLinks: 174,
	}

	if err := run(cfg); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(outputCAR); err != nil {
		t.Fatal("CAR file not created:", err)
	}

	t.Log("Large directory with HAMT sharding test passed")
}