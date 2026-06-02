// Package traverse provides merkledag traversal functions
package traverse

import (
	"context"
	"errors"

	"github.com/gammazero/deque"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// Visited tracks which CIDs a traversal has already seen, so it can skip
// duplicate subtrees when Options.SkipDuplicates is set.
//
// *github.com/ipfs/go-cid.Set already implements this interface. For very large
// DAGs you can plug in your own version (say, one backed by disk or a Bloom
// filter) to keep memory in check, or pass a set you already have so there is
// only one copy.
//
// Traverse calls Visit from a single goroutine, so your implementation does not
// need to be safe for concurrent use; the built-in *go-cid.Set is not. If you
// share one Visited between traversals that run at the same time, add your own
// locking.
type Visited interface {
	// Visit records c and returns true the first time c is seen, or false if c
	// was already in the set.
	Visit(c cid.Cid) bool
}

// Order is an identifier for traversal algorithm orders
type Order int

// These constants define different traversing methods
const (
	// DFSPre defines depth-first pre-order
	DFSPre Order = iota
	// DFSPost defines depth-first post-order
	DFSPost
	// BFS defines breadth-first order
	BFS
)

// Options specifies a series of traversal options
type Options struct {
	DAG     ipld.NodeGetter // the dagservice to fetch nodes
	Order   Order           // what order to traverse in
	Func    Func            // the function to perform at each step
	ErrFunc ErrFunc         // see ErrFunc. Optional

	SkipDuplicates bool // whether to skip duplicate nodes

	// Visited is the set used to skip duplicate nodes when SkipDuplicates is set.
	// If nil, an in-memory go-cid Set is used. It is ignored when SkipDuplicates
	// is false. A bounded or disk-backed set keeps memory low on very large DAGs.
	//
	// You can also pass a set that already holds some CIDs to skip them up front.
	// The root is special here: BFS checks it against the set, so a pre-seeded
	// root CID skips the whole walk, while the depth-first orders never skip the
	// root.
	Visited Visited
}

// State is a current traversal state
type State struct {
	Node  ipld.Node
	Depth int
}

type traversal struct {
	opts    Options
	visited Visited
}

func (t *traversal) shouldSkip(n ipld.Node) (bool, error) {
	if t.opts.SkipDuplicates {
		// Visit reports false when the CID was already recorded.
		if !t.visited.Visit(n.Cid()) {
			return true, nil
		}
	}

	return false, nil
}

func (t *traversal) callFunc(next State) error {
	return t.opts.Func(next)
}

// getNode returns the node for link. If it return an error,
// stop processing. if it returns a nil node, just skip it.
//
// the error handling is a little complicated.
func (t *traversal) getNode(link *ipld.Link) (ipld.Node, error) {
	getNode := func(l *ipld.Link) (ipld.Node, error) {
		next, err := l.GetNode(context.TODO(), t.opts.DAG)
		if err != nil {
			return nil, err
		}

		skip, err := t.shouldSkip(next)
		if skip {
			next = nil
		}
		return next, err
	}

	next, err := getNode(link)
	if err != nil && t.opts.ErrFunc != nil { // attempt recovery.
		err = t.opts.ErrFunc(err)
		next = nil // skip regardless
	}
	return next, err
}

// Func is the type of the function called for each dag.Node visited by Traverse.
// The traversal argument contains the current traversal state.
// If an error is returned, processing stops.
type Func func(current State) error

// ErrFunc is provided to handle problems when walking to the Node. Traverse
// will call ErrFunc with the error encountered. ErrFunc can decide how to
// handle that error, and return an error back to Traversal with how to proceed:
//   - nil - skip the Node and its children, but continue processing
//   - all other errors halt processing immediately.
//
// If ErrFunc is nil, Traversal will stop, as if:
//
//	opts.ErrFunc = func(err error) { return err }
type ErrFunc func(err error) error

// Traverse initiates a DAG traversal with the given options starting at
// the given root.
func Traverse(root ipld.Node, o Options) error {
	t := traversal{
		opts: o,
	}
	if o.SkipDuplicates {
		t.visited = o.Visited
		if t.visited == nil {
			t.visited = cid.NewSet()
		}
	}

	state := State{
		Node:  root,
		Depth: 0,
	}

	switch o.Order {
	default:
		return dfsPreTraverse(state, &t)
	case DFSPre:
		return dfsPreTraverse(state, &t)
	case DFSPost:
		return dfsPostTraverse(state, &t)
	case BFS:
		return bfsTraverse(state, &t)
	}
}

type dfsFunc func(state State, t *traversal) error

func dfsPreTraverse(state State, t *traversal) error {
	if err := t.callFunc(state); err != nil {
		return err
	}
	return dfsDescend(dfsPreTraverse, state, t)
}

func dfsPostTraverse(state State, t *traversal) error {
	if err := dfsDescend(dfsPostTraverse, state, t); err != nil {
		return err
	}
	return t.callFunc(state)
}

func dfsDescend(df dfsFunc, curr State, t *traversal) error {
	for _, l := range curr.Node.Links() {
		node, err := t.getNode(l)
		if err != nil {
			return err
		}
		if node == nil { // skip
			continue
		}

		next := State{
			Node:  node,
			Depth: curr.Depth + 1,
		}
		if err := df(next, t); err != nil {
			return err
		}
	}
	return nil
}

func bfsTraverse(root State, t *traversal) error {
	if skip, err := t.shouldSkip(root.Node); skip || err != nil {
		return err
	}

	var q deque.Deque[State]
	q.PushBack(root)
	for q.Len() > 0 {
		curr := q.PopFront()
		if curr.Node == nil {
			return errors.New("failed to dequeue though queue not empty")
		}

		// call user's func
		if err := t.callFunc(curr); err != nil {
			return err
		}

		for _, l := range curr.Node.Links() {
			node, err := t.getNode(l)
			if err != nil {
				return err
			}
			if node == nil { // skip
				continue
			}

			q.PushBack(State{
				Node:  node,
				Depth: curr.Depth + 1,
			})
		}
	}
	return nil
}
