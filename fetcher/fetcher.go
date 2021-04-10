package fetcher

import (
	"context"

	"github.com/ipld/go-ipld-prime"
)

// Fetcher is an interface for reading from a dag. Reads may be local or remote, and may employ data exchange
// protocols like graphsync and bitswap
type Fetcher interface {
	// NodeMatching traverses a node graph starting with the provided node using the given selector and possibly crossing
	// block boundaries. Each matched node is passed as FetchResult to the callback. Errors returned from callback will
	// halt the traversal. The sequence of events is: NodeMatching begins, the callback is called zero or more times
	// with a FetchResult, then NodeMatching returns.
	NodeMatching(context.Context, ipld.Node, ipld.Node, FetchCallback) error

	// BlockOfType fetches a node graph of the provided type corresponding to single block by link.
	BlockOfType(context.Context, ipld.Link, ipld.NodePrototype) (ipld.Node, error)

	// BlockMatchingOfType traverses a node graph starting with the given link using the given selector and possibly
	// crossing block boundaries. The nodes will be typed using the provided prototype. Each matched node is passed as
	// a FetchResult to the callback. Errors returned from callback will halt the traversal.
	// The sequence of events is: BlockMatchingOfType begins, the callback is called zero or more times with a
	// FetchResult, then BlockMatchingOfType returns.
	BlockMatchingOfType(context.Context, ipld.Link, ipld.Node, ipld.NodePrototype, FetchCallback) error

	// Uses the given link to pick a prototype to build the linked node.
	PrototypeFromLink(link ipld.Link) (ipld.NodePrototype, error)
}

// FetchResult is a single node read as part of a dag operation called on a fetcher
type FetchResult struct {
	Node          ipld.Node
	Path          ipld.Path
	LastBlockPath ipld.Path
	LastBlockLink ipld.Link
}

// FetchCallback is called for each node traversed during a fetch
type FetchCallback func(result FetchResult) error
