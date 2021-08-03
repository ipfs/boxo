package car_test

import (
	"io"
	"os"
	"testing"

	carv2 "github.com/ipld/go-car/v2"
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
