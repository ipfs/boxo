package traverse

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// Aggregate describes the distinct nodes reachable from a root. Weight is the
// sum of the caller-supplied weight of each node.
type Aggregate struct {
	Nodes  int64
	Weight uint64
}

// MemoizedWalker reuses traversal metadata across roots without retaining block
// payloads. Each root counts a CID once, even when its subgraphs overlap.
// It uses an explicit stack, so graph depth does not consume the Go call stack.
// Memory is O(V+E) in the union of the visited graphs, plus O(V) per Walk.
// Instances are intended for one operation and are not safe for concurrent use.
type MemoizedWalker struct {
	getter ipld.NodeGetter
	weight func(ipld.Node) uint64
	index  map[cid.Cid]int
	nodes  []memoNode
	// Prefix sums let closed subgraphs be aggregated without visiting each node.
	weights []uint64
}

type memoNode struct {
	links []cid.Cid
	// Discovery indexes in [index, end) form this node's DFS subtree. If low
	// equals index, all reachable nodes are in that interval. Otherwise a link
	// reaches an earlier subtree and we must follow the cached links instead.
	low, end int
}

// NewMemoizedWalker creates an opt-in traversal cache. weight must be a pure
// function of the node; nil assigns zero weight and only counts nodes. A caller
// computing byte sizes can use len(node.RawData()). Existing Traverse behavior
// is unaffected. The cache is discarded after an unsuccessful Walk.
func NewMemoizedWalker(getter ipld.NodeGetter, weight func(ipld.Node) uint64) *MemoizedWalker {
	return &MemoizedWalker{getter: getter, weight: weight, index: make(map[cid.Cid]int), weights: []uint64{0}}
}

// Total returns the aggregate of all distinct nodes discovered across roots.
func (m *MemoizedWalker) Total() Aggregate {
	return Aggregate{Nodes: int64(len(m.nodes)), Weight: m.weights[len(m.weights)-1]}
}

// Walk returns the aggregate for root. progress, when non-nil, receives the
// cumulative aggregate for this root after each new node or cached subgraph.
// Its errors and context cancellation stop the traversal, including cache hits.
// A cache hit may combine multiple nodes into one progress update.
func (m *MemoizedWalker) Walk(ctx context.Context, root cid.Cid, progress func(Aggregate) error) (result Aggregate, err error) {
	defer func() {
		if err != nil {
			// Never reuse incomplete DFS intervals after a failed traversal.
			m.index = make(map[cid.Cid]int)
			m.nodes = nil
			m.weights = []uint64{0}
		}
	}()
	var seen memoIntervals
	enter := func(c cid.Cid) (int, bool, error) {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		i, ok := m.index[c]
		if !ok {
			node, err := m.getter.Get(ctx, c)
			if err != nil {
				return 0, false, fmt.Errorf("getting %s: %w", c, err)
			}
			i = len(m.nodes)
			links := node.Links()
			entry := memoNode{low: i, links: make([]cid.Cid, len(links))}
			for j, link := range links {
				entry.links[j] = link.Cid
			}
			var weight uint64
			if m.weight != nil {
				weight = m.weight(node)
			}
			m.index[c] = i
			m.nodes = append(m.nodes, entry)
			m.weights = append(m.weights, m.weights[i]+weight)
		}
		entry := &m.nodes[i]
		closed := entry.end > i && entry.low == i
		end := i + 1
		if closed {
			end = entry.end
		}
		delta := seen.add(i, end, m.weights)
		if delta.Nodes == 0 {
			return i, false, nil
		}
		result.Nodes += delta.Nodes
		result.Weight += delta.Weight
		if progress != nil {
			if err := progress(result); err != nil {
				return i, false, err
			}
		}
		return i, !closed, nil
	}

	i, descend, err := enter(root)
	if err != nil {
		return Aggregate{}, err
	}
	type frame struct{ node, next int }
	var stack []frame
	if descend {
		stack = append(stack, frame{node: i})
	}
	for len(stack) != 0 {
		if err := ctx.Err(); err != nil {
			return Aggregate{}, err
		}
		top := &stack[len(stack)-1]
		i := top.node
		if top.next < len(m.nodes[i].links) {
			link := m.nodes[i].links[top.next]
			top.next++
			child, descend, err := enter(link)
			if err != nil {
				return Aggregate{}, err
			}
			if descend {
				stack = append(stack, frame{node: child})
			} else if m.nodes[i].end == 0 {
				m.nodes[i].low = min(m.nodes[i].low, m.nodes[child].low)
			}
			continue
		}
		if m.nodes[i].end == 0 {
			m.nodes[i].end = len(m.nodes)
		}
		stack = stack[:len(stack)-1]
		if len(stack) != 0 {
			parent := stack[len(stack)-1].node
			if m.nodes[parent].end == 0 {
				m.nodes[parent].low = min(m.nodes[parent].low, m.nodes[i].low)
			}
		}
	}
	return result, ctx.Err()
}

type memoInterval struct{ start, end int }
type memoIntervals []memoInterval

// add marks a range as visited and reports only the previously unseen weight.
// Sorted, disjoint intervals avoid storing a transitive CID set at every node
// (quadratic space on a chain). Adjacent ranges are coalesced.
func (s *memoIntervals) add(start, end int, weights []uint64) Aggregate {
	result := Aggregate{Nodes: int64(end - start), Weight: weights[end] - weights[start]}
	i := sort.Search(len(*s), func(i int) bool { return (*s)[i].end >= start })
	j := i
	merged := memoInterval{start, end}
	for j < len(*s) && (*s)[j].start <= merged.end {
		interval := (*s)[j]
		lo, hi := max(start, interval.start), min(end, interval.end)
		if lo < hi {
			result.Nodes -= int64(hi - lo)
			result.Weight -= weights[hi] - weights[lo]
		}
		merged.start = min(merged.start, interval.start)
		merged.end = max(merged.end, interval.end)
		j++
	}
	*s = slices.Replace(*s, i, j, merged)
	return result
}
