package traverse

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	mdag "github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/require"
)

type memoGetter struct {
	ipld.NodeGetter
	nodes map[cid.Cid]ipld.Node
	gets  map[cid.Cid]int
}

func (g *memoGetter) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.gets[c]++
	node, ok := g.nodes[c]
	if !ok {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	return node, nil
}

func memoGraph(t testing.TB, edges [][]int) (*memoGetter, []cid.Cid) {
	t.Helper()
	g := &memoGetter{nodes: make(map[cid.Cid]ipld.Node), gets: make(map[cid.Cid]int)}
	roots := make([]cid.Cid, len(edges))
	for i := len(edges) - 1; i >= 0; i-- {
		node := mdag.NodeWithData(fmt.Appendf(nil, "node %d", i))
		for j, child := range edges[i] {
			require.Greater(t, child, i)
			require.NoError(t, node.AddRawLink(fmt.Sprint(j), &ipld.Link{Cid: roots[child]}))
		}
		roots[i] = node.Cid()
		g.nodes[node.Cid()] = node
	}
	return g, roots
}

func memoWeight(node ipld.Node) uint64 { return uint64(len(node.RawData())) }

// Independent, iterative reference with a plain per-root CID set.
func referenceAggregate(g *memoGetter, root cid.Cid) Aggregate {
	var result Aggregate
	seen := cid.NewSet()
	pending := []cid.Cid{root}
	for len(pending) != 0 {
		c := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if !seen.Visit(c) {
			continue
		}
		node := g.nodes[c]
		result.Nodes++
		result.Weight += memoWeight(node)
		for _, link := range node.Links() {
			pending = append(pending, link.Cid)
		}
	}
	return result
}

func TestMemoizedWalkerOverlappingSubgraphs(t *testing.T) {
	// Diamond, duplicate links, overlapping roots, and a new root containing a
	// previously cached subgraph. All roots must deduplicate independently.
	g, roots := memoGraph(t, [][]int{{2, 3, 2}, {3, 4}, {4, 5}, {4}, {5}, {}})
	memo := NewMemoizedWalker(g, memoWeight)
	for _, root := range []cid.Cid{roots[2], roots[0], roots[1], roots[0], roots[4], roots[3]} {
		got, err := memo.Walk(t.Context(), root, nil)
		require.NoError(t, err)
		require.Equal(t, referenceAggregate(g, root), got)
	}
	var total Aggregate
	for c, node := range g.nodes {
		require.Equal(t, 1, g.gets[c], "CID %s should only be fetched once", c)
		total.Nodes++
		total.Weight += memoWeight(node)
	}
	require.Equal(t, total, memo.Total())
}

func TestMemoizedWalkerRandomDAGs(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for range 30 {
		edges := make([][]int, 64)
		for i := range edges[:len(edges)-1] {
			for range rng.Intn(8) {
				edges[i] = append(edges[i], i+1+rng.Intn(len(edges)-i-1))
			}
		}
		g, roots := memoGraph(t, edges)
		memo := NewMemoizedWalker(g, memoWeight)
		for _, i := range rng.Perm(len(roots)) {
			got, err := memo.Walk(t.Context(), roots[i], nil)
			require.NoError(t, err)
			require.Equal(t, referenceAggregate(g, roots[i]), got)
		}
		for _, gets := range g.gets {
			require.Equal(t, 1, gets)
		}
	}
}

func TestMemoizedWalkerDeepAndShared(t *testing.T) {
	const depth = 50000
	edges := make([][]int, depth+2)
	// Two distinct roots sharing a long chain.
	edges[0], edges[1] = []int{2}, []int{2}
	for i := 2; i < len(edges)-1; i++ {
		edges[i] = []int{i + 1}
	}
	g, roots := memoGraph(t, edges)
	memo := NewMemoizedWalker(g, memoWeight)
	first, err := memo.Walk(t.Context(), roots[0], nil)
	require.NoError(t, err)
	require.EqualValues(t, depth+1, first.Nodes)
	updates := 0
	var previous Aggregate
	second, err := memo.Walk(t.Context(), roots[1], func(current Aggregate) error {
		require.Greater(t, current.Nodes, previous.Nodes)
		require.Greater(t, current.Weight, previous.Weight)
		previous = current
		updates++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, referenceAggregate(g, roots[1]), second)
	require.Equal(t, 2, updates, "one update for the new root and one for the entire cached chain")
	require.Len(t, memo.nodes, depth+2, "cache size must stay linear on a deep chain")
	for _, gets := range g.gets {
		require.Equal(t, 1, gets)
	}
}

func TestMemoizedWalkerErrors(t *testing.T) {
	t.Run("missing node does not poison cache", func(t *testing.T) {
		g, roots := memoGraph(t, [][]int{{1}, {}})
		leaf := g.nodes[roots[1]]
		delete(g.nodes, roots[1])
		memo := NewMemoizedWalker(g, memoWeight)
		_, err := memo.Walk(t.Context(), roots[0], nil)
		require.True(t, ipld.IsNotFound(err))
		require.Equal(t, Aggregate{}, memo.Total())
		g.nodes[roots[1]] = leaf
		got, err := memo.Walk(t.Context(), roots[0], nil)
		require.NoError(t, err)
		require.Equal(t, referenceAggregate(g, roots[0]), got)
	})
	t.Run("progress error on cache hit", func(t *testing.T) {
		g, roots := memoGraph(t, [][]int{{1}, {}})
		memo := NewMemoizedWalker(g, nil)
		got, err := memo.Walk(t.Context(), roots[0], nil)
		require.NoError(t, err)
		require.Equal(t, Aggregate{Nodes: 2}, got)
		stop := errors.New("stop emitting")
		_, err = memo.Walk(t.Context(), roots[0], func(Aggregate) error { return stop })
		require.ErrorIs(t, err, stop)
		require.Equal(t, Aggregate{}, memo.Total())
	})
	t.Run("cancellation during traversal and on cache hit", func(t *testing.T) {
		g, roots := memoGraph(t, [][]int{{1}, {}})
		memo := NewMemoizedWalker(g, memoWeight)
		ctx, cancel := context.WithCancel(t.Context())
		_, err := memo.Walk(ctx, roots[0], func(Aggregate) error { cancel(); return nil })
		require.ErrorIs(t, err, context.Canceled)
		_, err = memo.Walk(t.Context(), roots[0], nil)
		require.NoError(t, err)
		_, err = memo.Walk(ctx, roots[0], nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func BenchmarkMemoizedWalker(b *testing.B) {
	for _, shape := range []string{"single", "shared", "disjoint"} {
		const rootsCount, depth = 20, 256
		edges := make([][]int, rootsCount+rootsCount*depth)
		for root := range rootsCount {
			start := rootsCount + root*depth
			edges[root] = []int{start}
			if shape == "shared" {
				edges[root] = []int{rootsCount}
			}
			for i := start; i < start+depth-1; i++ {
				edges[i] = []int{i + 1}
			}
		}
		for _, memoize := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/memoize=%v", shape, memoize), func(b *testing.B) {
				g, roots := memoGraph(b, edges)
				roots = roots[:rootsCount]
				if shape == "single" {
					roots = roots[:1]
				}
				b.ReportAllocs()
				for b.Loop() {
					if memoize {
						memo := NewMemoizedWalker(g, memoWeight)
						for _, root := range roots {
							if _, err := memo.Walk(b.Context(), root, nil); err != nil {
								b.Fatal(err)
							}
						}
					} else {
						for _, root := range roots {
							var result Aggregate
							err := Traverse(g.nodes[root], Options{DAG: g, Order: DFSPre, SkipDuplicates: true, Func: func(s State) error {
								result.Nodes++
								result.Weight += memoWeight(s.Node)
								return nil
							}})
							if err != nil {
								b.Fatal(err)
							}
						}
					}
				}
			})
		}
	}
}
