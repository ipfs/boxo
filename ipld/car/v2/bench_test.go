package car_test

import (
	"context"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/boxo/ipld/car/v2/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"

	carv2 "github.com/ipfs/boxo/ipld/car/v2"
)

// BenchmarkReadBlocks instantiates a BlockReader, and iterates over all blocks.
// It essentially looks at the contents of any CARv1 or CARv2 file.
// Note that this also uses internal carv1.ReadHeader underneath.
func BenchmarkReadBlocks(b *testing.B) {
	path := "testdata/sample-wrapped-v2.car"

	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(info.Size())
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r, err := os.Open("testdata/sample-wrapped-v2.car")
			if err != nil {
				b.Fatal(err)
			}
			br, err := carv2.NewBlockReader(r)
			if err != nil {
				b.Fatal(err)
			}
			for {
				_, err := br.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					b.Fatal(err)
				}
			}

			if err := r.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkExtractV1File extracts inner CARv1 payload from a sample CARv2 file using ExtractV1File.
func BenchmarkExtractV1File(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench-large-v2.car")
	generateRandomCarV2File(b, path, 10<<20) // 10 MiB
	defer os.Remove(path)

	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(info.Size())
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		dstPath := filepath.Join(b.TempDir(), "destination.car")
		for pb.Next() {
			err = carv2.ExtractV1File(path, dstPath)
			if err != nil {
				b.Fatal(err)
			}
			_ = os.Remove(dstPath)
		}
	})
}

// BenchmarkExtractV1UsingReader extracts inner CARv1 payload from a sample CARv2 file using Reader
// API. This benchmark is implemented to be used as a comparison in conjunction with
// BenchmarkExtractV1File.
func BenchmarkExtractV1UsingReader(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench-large-v2.car")
	generateRandomCarV2File(b, path, 10<<20) // 10 MiB
	defer os.Remove(path)

	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(info.Size())
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		dstPath := filepath.Join(b.TempDir(), "destination.car")
		for pb.Next() {
			dst, err := os.Create(dstPath)
			if err != nil {
				b.Fatal(err)
			}
			reader, err := carv2.OpenReader(path)
			if err != nil {
				b.Fatal(err)
			}
			dr, err := reader.DataReader()
			if err != nil {
				b.Fatal(err)
			}
			_, err = io.Copy(dst, dr)
			if err != nil {
				b.Fatal(err)
			}
			if err := dst.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkReader_InspectWithBlockValidation benchmarks Reader.Inspect with block hash validation
// for a randomly generated CARv2 file of size 10 MiB.
func BenchmarkReader_InspectWithBlockValidation(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench-large-v2.car")
	generateRandomCarV2File(b, path, 10<<20) // 10 MiB
	defer os.Remove(path)

	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(info.Size())
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchmarkInspect(b, path, true)
		}
	})
}

// BenchmarkReader_InspectWithoutBlockValidation benchmarks Reader.Inspect without block hash
// validation for a randomly generated CARv2 file of size 10 MiB.
func BenchmarkReader_InspectWithoutBlockValidation(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench-large-v2.car")
	generateRandomCarV2File(b, path, 10<<20) // 10 MiB
	defer os.Remove(path)

	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(info.Size())
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchmarkInspect(b, path, false)
		}
	})
}

func benchmarkInspect(b *testing.B, path string, validateBlockHash bool) {
	reader, err := carv2.OpenReader(path)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := reader.Inspect(validateBlockHash); err != nil {
		b.Fatal(err)
	}
}
func generateRandomCarV2File(b *testing.B, path string, minTotalBlockSize int) {
	// Use fixed RNG for determinism across benchmarks.
	rng := rand.New(rand.NewSource(1413))
	bs, err := blockstore.OpenReadWrite(path, []cid.Cid{})
	defer func() {
		if err := bs.Finalize(); err != nil {
			b.Fatal(err)
		}
	}()
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 32<<10) // 32 KiB
	var totalBlockSize int
	for totalBlockSize < minTotalBlockSize {
		size, err := rng.Read(buf)
		if err != nil {
			b.Fatal(err)
		}

		blk := merkledag.NewRawNode(buf)
		if err := bs.Put(context.TODO(), blk); err != nil {
			b.Fatal(err)
		}
		totalBlockSize += size
	}
}
