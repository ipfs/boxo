package blockstore_test

import (
	"io"
	mathrand "math/rand"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
)

// Open a read-only blockstore,
// and retrieve all blocks in a shuffled order.
// Note that this benchmark includes generating an index,
// since the input file is a CARv1.

func BenchmarkOpenReadOnlyV1(b *testing.B) {
	path := "../testdata/sample-v1.car"

	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(info.Size())
	b.ReportAllocs()

	var shuffledCIDs []cid.Cid
	cr, err := carv2.OpenReader(path)
	if err != nil {
		b.Fatal(err)
	}
	for {
		block, err := cr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Fatal(err)
		}
		shuffledCIDs = append(shuffledCIDs, block.Cid())
	}
	cr.Close()

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
				_, err := bs.Get(c)
				if err != nil {
					b.Fatal(err)
				}
			}

			bs.Close()
		}
	})
}
