package testutil

import (
	"testing"

	blocks "github.com/ipfs/go-block-format"
)

func TestGenerateBlocksOfSize(t *testing.T) {
	for _, b1 := range GenerateBlocksOfSize(10, 100) {
		b2 := blocks.NewBlock(b1.RawData())
		if b2.Cid() != b1.Cid() {
			t.Fatal("block CIDs mismatch")
		}
	}
}
