package blockstore_test

import (
	"context"
	"io"
	mathrand "math/rand"
	"os"
	"testing"

	carv2 "github.com/ipfs/boxo/ipld/car/v2"
	"github.com/ipfs/boxo/ipld/car/v2/blockstore"
	"github.com/ipfs/go-cid"
)

// BenchmarkOpenReadOnlyV1 opens a read-only blockstore,
// and retrieves all blocks in a shuffled order.
// Note that this benchmark includes generating an index,
// since the input file is a CARv1.
func BenchmarkOpenReadOnlyV1(b *testing.B) {
	path := "../testdata/sample-v1.car"
	f, err := os.Open("../testdata/sample-v1.car")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			b.Fatal(err)
		}
	}()
	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(info.Size())
	b.ReportAllocs()

	var shuffledCIDs []cid.Cid
	br, err := carv2.NewBlockReader(f)
	if err != nil {
		b.Fatal(err)
	}
	for {
		block, err := br.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Fatal(err)
		}
		shuffledCIDs = append(shuffledCIDs, block.Cid())
	}

	// The shuffling needs to be deterministic,
	// for the sake of stable benchmark results.
	// Any source number works as long as it's fixed.
	rnd := mathrand.New(mathrand.NewSource(123456))
	rnd.Shuffle(len(shuffledCIDs), func(i, j int) {
		shuffledCIDs[i], shuffledCIDs[j] = shuffledCIDs[j], shuffledCIDs[i]
	})

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bs, err := blockstore.OpenReadOnly(path)
			if err != nil {
				b.Fatal(err)
			}

			for _, c := range shuffledCIDs {
				_, err := bs.Get(context.TODO(), c)
				if err != nil {
					b.Fatal(err)
				}
			}

			if err := bs.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
