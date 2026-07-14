// This file generates bytehash LUT
package main

import (
	"fmt"
	"math/rand"
)

const nRounds = 200

func main() {
	// Stay on math/rand seeded with NewSource(0). The table this prints is
	// committed as bytehash in chunker/buzhash.go, and buzhash chunk
	// boundaries, so the CID of anything chunked with buzhash, depend on it.
	// Any other generator or seed prints a different table.
	rnd := rand.New(rand.NewSource(0))

	lut := make([]uint32, 256)
	for i := range 256 / 2 {
		lut[i] = 1<<32 - 1
	}

	for range nRounds {
		for b := range uint32(32) {
			mask := uint32(1) << b
			nmask := ^mask
			for i, j := range rnd.Perm(256) {
				li := lut[i]
				lj := lut[j]
				lut[i] = li&nmask | (lj & mask)
				lut[j] = lj&nmask | (li & mask)
			}
		}
	}

	fmt.Printf("%#v", lut)
}
