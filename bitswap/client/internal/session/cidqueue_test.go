package session

import (
	"testing"

	"github.com/ipfs/go-test/random"
)

func TestCidQueueGC(t *testing.T) {
	cq := newCidQueue()

	for _, c := range random.Cids(5) {
		cq.push(c)
		cq.remove(c)
	}

	cq.gc()

	if cq.elems.Len() != 0 {
		t.Fatal("all elements were not removed")
	}
}
