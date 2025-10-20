package main

import (
	"os"
	"path/filepath"
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
	if err := run(testFile, outputCAR, "sha2-256"); err != nil {
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

func TestInvalidHashFunction(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	outputCAR := filepath.Join(tmpDir, "output.car")
	err := run(testFile, outputCAR, "invalid-hash-function")
	if err == nil {
		t.Fatal("Expected error for invalid hash function")
	}

	if err.Error() != "unknown hash function: invalid-hash-function" {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestDirectoryRejection(t *testing.T) {
	tmpDir := t.TempDir()

	outputCAR := filepath.Join(tmpDir, "output.car")
	err := run(tmpDir, outputCAR, "sha2-256")
	if err == nil {
		t.Fatal("Expected error for directory input")
	}

	if err.Error() != "directory support not yet implemented" {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestNonExistentFile(t *testing.T) {
	tmpDir := t.TempDir()

	outputCAR := filepath.Join(tmpDir, "output.car")
	err := run("/nonexistent/file.txt", outputCAR, "sha2-256")
	if err == nil {
		t.Fatal("Expected error for non-existent file")
	}
}