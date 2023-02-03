package ipsl

import (
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
)

type CidTraversalPair struct {
	Cid       cid.Cid
	Traversal Traversal
}

type Traversal interface {
	// Traverse must never be called with bytes not matching the cid.
	// The bytes must never be modified by the implementations.
	Traverse(blocks.Block) ([]CidTraversalPair, error)
}

// An AllNode traverse all the traversals with the same cid it is given to.
type AllNode struct {
	Traversals []Traversal
}

func All(traversals ...Traversal) Traversal {
	return AllNode{traversals}
}

func (n AllNode) Traverse(b blocks.Block) ([]CidTraversalPair, error) {
	var results []CidTraversalPair
	for _, t := range n.Traversals {
		r, err := t.Traverse(b)
		if err != nil {
			return nil, err
		}
		results = append(results, r...)
	}
	return results, nil
}

// EmptyTraversal is a traversal that always returns nothing
type EmptyTraversal struct{}

func Empty() Traversal {
	return EmptyTraversal{}
}

func (c EmptyTraversal) Traverse(_ blocks.Block) ([]CidTraversalPair, error) {
	return []CidTraversalPair{}, nil
}
