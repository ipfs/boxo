package session

import (
	"github.com/gammazero/deque"
	cid "github.com/ipfs/go-cid"
)

type cidQueue struct {
	elems deque.Deque[cid.Cid]
	eset  *cid.Set
}

func newCidQueue() *cidQueue {
	return &cidQueue{eset: cid.NewSet()}
}

func (cq *cidQueue) Pop() cid.Cid {
	for {
		if cq.elems.Len() == 0 {
			return cid.Cid{}
		}

		out := cq.elems.PopFront()

		if cq.eset.Has(out) {
			cq.eset.Remove(out)
			return out
		}
	}
}

func (cq *cidQueue) Cids() []cid.Cid {
	// Lazily delete from the list any cids that were removed from the set
	if cq.elems.Len() > cq.eset.Len() {
		for i := 0; i < cq.elems.Len(); i++ {
			c := cq.elems.PopFront()
			if cq.eset.Has(c) {
				cq.elems.PushBack(c)
			}
		}
	}

	if cq.elems.Len() == 0 {
		return nil
	}

	// Make a copy of the cids
	cids := make([]cid.Cid, cq.elems.Len())
	for i := 0; i < cq.elems.Len(); i++ {
		cids[i] = cq.elems.At(i)
	}
	return cids
}

func (cq *cidQueue) Push(c cid.Cid) {
	if cq.eset.Visit(c) {
		cq.elems.PushBack(c)
	}
}

func (cq *cidQueue) Remove(c cid.Cid) {
	cq.eset.Remove(c)
}

func (cq *cidQueue) Has(c cid.Cid) bool {
	return cq.eset.Has(c)
}

func (cq *cidQueue) Len() int {
	return cq.eset.Len()
}
