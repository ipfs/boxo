package car_test

import (
	"io"
	"os"
	"testing"

	carv2 "github.com/ipld/go-car/v2"
)

// Open a reader, get the roots, and iterate over all blocks.
// Essentially looking at the contents of any CARv1 or CARv2 file.
// Note that this also uses ReadVersion underneath.

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
			cr, err := carv2.OpenReader(path)
			if err != nil {
				b.Fatal(err)
			}
			_, err = cr.Roots()
			if err != nil {
				b.Fatal(err)
			}
			for {
				_, err := cr.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					b.Fatal(err)
				}
			}

			cr.Close()
		}
	})
}
