// This file generates bytehash LUT
package main

import (
	"fmt"
	"math/rand"
)

const nRounds = 200

func main() {
	rnd := rand.New(rand.NewSource(0))

	lut := make([]uint32, 256)
	for i := 0; i < 256/2; i++ {
		lut[i] = 1<<32 - 1
	}

	for r := 0; r < nRounds; r++ {
		for b := uint32(0); b < 32; b++ {
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
