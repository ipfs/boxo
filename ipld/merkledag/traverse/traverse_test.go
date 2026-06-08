package traverse

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	mdag "github.com/ipfs/boxo/ipld/merkledag"
	mdagtest "github.com/ipfs/boxo/ipld/merkledag/test"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

func TestDFSPreNoSkip(t *testing.T) {
	ds := mdagtest.Mock()
	opts := Options{Order: DFSPre, DAG: ds}

	testWalkOutputs(t, newFan(t, ds), opts, []byte(`
0 /a
1 /a/aa
1 /a/ab
1 /a/ac
1 /a/ad
`))

	testWalkOutputs(t, newLinkedList(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
`))

	testWalkOutputs(t, newBinaryTree(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
2 /a/aa/aab
1 /a/ab
2 /a/ab/aba
2 /a/ab/abb
`))

	testWalkOutputs(t, newBinaryDAG(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
`))
}

func TestDFSPreSkip(t *testing.T) {
	ds := mdagtest.Mock()
	opts := Options{Order: DFSPre, SkipDuplicates: true, DAG: ds}

	testWalkOutputs(t, newFan(t, ds), opts, []byte(`
0 /a
1 /a/aa
1 /a/ab
1 /a/ac
1 /a/ad
`))

	testWalkOutputs(t, newLinkedList(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
`))

	testWalkOutputs(t, newBinaryTree(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
2 /a/aa/aab
1 /a/ab
2 /a/ab/aba
2 /a/ab/abb
`))

	testWalkOutputs(t, newBinaryDAG(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
`))
}

func TestDFSPostNoSkip(t *testing.T) {
	ds := mdagtest.Mock()
	opts := Options{Order: DFSPost, DAG: ds}

	testWalkOutputs(t, newFan(t, ds), opts, []byte(`
1 /a/aa
1 /a/ab
1 /a/ac
1 /a/ad
0 /a
`))

	testWalkOutputs(t, newLinkedList(t, ds), opts, []byte(`
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
2 /a/aa/aaa
1 /a/aa
0 /a
`))

	testWalkOutputs(t, newBinaryTree(t, ds), opts, []byte(`
2 /a/aa/aaa
2 /a/aa/aab
1 /a/aa
2 /a/ab/aba
2 /a/ab/abb
1 /a/ab
0 /a
`))

	testWalkOutputs(t, newBinaryDAG(t, ds), opts, []byte(`
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
2 /a/aa/aaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
2 /a/aa/aaa
1 /a/aa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
2 /a/aa/aaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
2 /a/aa/aaa
1 /a/aa
0 /a
`))
}

func TestDFSPostSkip(t *testing.T) {
	ds := mdagtest.Mock()
	opts := Options{Order: DFSPost, SkipDuplicates: true, DAG: ds}

	testWalkOutputs(t, newFan(t, ds), opts, []byte(`
1 /a/aa
1 /a/ab
1 /a/ac
1 /a/ad
0 /a
`))

	testWalkOutputs(t, newLinkedList(t, ds), opts, []byte(`
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
2 /a/aa/aaa
1 /a/aa
0 /a
`))

	testWalkOutputs(t, newBinaryTree(t, ds), opts, []byte(`
2 /a/aa/aaa
2 /a/aa/aab
1 /a/aa
2 /a/ab/aba
2 /a/ab/abb
1 /a/ab
0 /a
`))

	testWalkOutputs(t, newBinaryDAG(t, ds), opts, []byte(`
4 /a/aa/aaa/aaaa/aaaaa
3 /a/aa/aaa/aaaa
2 /a/aa/aaa
1 /a/aa
0 /a
`))
}

func TestBFSNoSkip(t *testing.T) {
	ds := mdagtest.Mock()
	opts := Options{Order: BFS, DAG: ds}

	testWalkOutputs(t, newFan(t, ds), opts, []byte(`
0 /a
1 /a/aa
1 /a/ab
1 /a/ac
1 /a/ad
`))

	testWalkOutputs(t, newLinkedList(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
`))

	testWalkOutputs(t, newBinaryTree(t, ds), opts, []byte(`
0 /a
1 /a/aa
1 /a/ab
2 /a/aa/aaa
2 /a/aa/aab
2 /a/ab/aba
2 /a/ab/abb
`))

	testWalkOutputs(t, newBinaryDAG(t, ds), opts, []byte(`
0 /a
1 /a/aa
1 /a/aa
2 /a/aa/aaa
2 /a/aa/aaa
2 /a/aa/aaa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
3 /a/aa/aaa/aaaa
3 /a/aa/aaa/aaaa
3 /a/aa/aaa/aaaa
3 /a/aa/aaa/aaaa
3 /a/aa/aaa/aaaa
3 /a/aa/aaa/aaaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
4 /a/aa/aaa/aaaa/aaaaa
`))
}

func TestBFSSkip(t *testing.T) {
	ds := mdagtest.Mock()
	opts := Options{Order: BFS, SkipDuplicates: true, DAG: ds}

	testWalkOutputs(t, newFan(t, ds), opts, []byte(`
0 /a
1 /a/aa
1 /a/ab
1 /a/ac
1 /a/ad
`))

	testWalkOutputs(t, newLinkedList(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
`))

	testWalkOutputs(t, newBinaryTree(t, ds), opts, []byte(`
0 /a
1 /a/aa
1 /a/ab
2 /a/aa/aaa
2 /a/aa/aab
2 /a/ab/aba
2 /a/ab/abb
`))

	testWalkOutputs(t, newBinaryDAG(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
`))
}

// countingVisited wraps a *cid.Set and counts Visit calls, so a test can confirm
// the traversal really uses the Visited we passed in.
type countingVisited struct {
	set   *cid.Set
	calls int
}

func (c *countingVisited) Visit(k cid.Cid) bool {
	c.calls++
	return c.set.Visit(k)
}

func TestInjectedVisitedIsConsulted(t *testing.T) {
	ds := mdagtest.Mock()
	v := &countingVisited{set: cid.NewSet()}
	opts := Options{Order: DFSPre, SkipDuplicates: true, DAG: ds, Visited: v}

	// newBinaryDAG links every node twice. With duplicates skipped, the DFSPre
	// walk collapses to one line of distinct nodes. Seeing that output means our
	// injected set, not some internal default, did the skipping.
	testWalkOutputs(t, newBinaryDAG(t, ds), opts, []byte(`
0 /a
1 /a/aa
2 /a/aa/aaa
3 /a/aa/aaa/aaaa
4 /a/aa/aaa/aaaa/aaaaa
`))

	if v.calls == 0 {
		t.Fatal("injected Visited was never consulted")
	}
}

func TestPrePopulatedVisitedSkipsSubtree(t *testing.T) {
	ds := mdagtest.Mock()

	a := mdag.NodeWithData([]byte("/a"))
	aa := child(t, ds, a, "aa")
	ab := child(t, ds, a, "ab")
	ac := child(t, ds, a, "ac")
	addLink(t, ds, a, aa)
	addLink(t, ds, a, ab)
	addLink(t, ds, a, ac)

	// Put ab in the set up front, so the walk treats it as already seen and
	// skips it.
	seen := cid.NewSet()
	seen.Add(ab.Cid())

	opts := Options{Order: BFS, SkipDuplicates: true, DAG: ds, Visited: seen}
	testWalkOutputs(t, a, opts, []byte(`
0 /a
1 /a/aa
1 /a/ac
`))
}

// TestPrePopulatedVisitedRootAsymmetry locks in a gotcha for anyone who passes a
// Visited set with CIDs already in it to skip them: the root is handled
// differently per order. The default depth-first walk always visits the root,
// but BFS checks the root just like any other node. So if you seed the root CID
// expecting depth-first behavior and then switch to BFS, you get no output at
// all. Read the BFS branch below before reusing a seeded set with a non-DFS walk.
func TestPrePopulatedVisitedRootAsymmetry(t *testing.T) {
	ds := mdagtest.Mock()
	root := newFan(t, ds)

	// Default order: DFSPre never checks the root against the set (it runs the
	// root's Func right away), so seeding the root CID does not skip the root.
	// Only the children are checked.
	dfsSeen := cid.NewSet()
	dfsSeen.Add(root.Cid())
	testWalkOutputs(t, root, Options{Order: DFSPre, SkipDuplicates: true, DAG: ds, Visited: dfsSeen}, []byte(`
0 /a
1 /a/aa
1 /a/ab
1 /a/ac
1 /a/ad
`))

	// Gotcha: BFS checks the root just like any other node, so the same seed
	// skips the whole walk. Reusing a DFS-style seeded set with BFS drops the
	// root, and you get nothing.
	bfsSeen := cid.NewSet()
	bfsSeen.Add(root.Cid())
	testWalkOutputs(t, root, Options{Order: BFS, SkipDuplicates: true, DAG: ds, Visited: bfsSeen}, []byte(""))
}

func testWalkOutputs(t *testing.T, root ipld.Node, opts Options, expect []byte) {
	expect = bytes.TrimLeft(expect, "\n")

	buf := new(bytes.Buffer)
	walk := func(current State) error {
		s := fmt.Sprintf("%d %s\n", current.Depth, current.Node.(*mdag.ProtoNode).Data())
		t.Logf("walk: %s", s)
		buf.Write([]byte(s))
		return nil
	}

	opts.Func = walk
	if err := Traverse(root, opts); err != nil {
		t.Error(err)
		return
	}

	actual := buf.Bytes()
	if !bytes.Equal(actual, expect) {
		t.Error("error: outputs differ")
		t.Logf("expect:\n%s", expect)
		t.Logf("actual:\n%s", actual)
	} else {
		t.Logf("expect matches actual:\n%s", expect)
	}
}

func newFan(t *testing.T, ds ipld.DAGService) ipld.Node {
	a := mdag.NodeWithData([]byte("/a"))
	addLink(t, ds, a, child(t, ds, a, "aa"))
	addLink(t, ds, a, child(t, ds, a, "ab"))
	addLink(t, ds, a, child(t, ds, a, "ac"))
	addLink(t, ds, a, child(t, ds, a, "ad"))
	return a
}

func newLinkedList(t *testing.T, ds ipld.DAGService) ipld.Node {
	a := mdag.NodeWithData([]byte("/a"))
	aa := child(t, ds, a, "aa")
	aaa := child(t, ds, aa, "aaa")
	aaaa := child(t, ds, aaa, "aaaa")
	aaaaa := child(t, ds, aaaa, "aaaaa")
	addLink(t, ds, aaaa, aaaaa)
	addLink(t, ds, aaa, aaaa)
	addLink(t, ds, aa, aaa)
	addLink(t, ds, a, aa)
	return a
}

func newBinaryTree(t *testing.T, ds ipld.DAGService) ipld.Node {
	a := mdag.NodeWithData([]byte("/a"))
	aa := child(t, ds, a, "aa")
	ab := child(t, ds, a, "ab")
	addLink(t, ds, aa, child(t, ds, aa, "aaa"))
	addLink(t, ds, aa, child(t, ds, aa, "aab"))
	addLink(t, ds, ab, child(t, ds, ab, "aba"))
	addLink(t, ds, ab, child(t, ds, ab, "abb"))
	addLink(t, ds, a, aa)
	addLink(t, ds, a, ab)
	return a
}

func newBinaryDAG(t *testing.T, ds ipld.DAGService) ipld.Node {
	a := mdag.NodeWithData([]byte("/a"))
	aa := child(t, ds, a, "aa")
	aaa := child(t, ds, aa, "aaa")
	aaaa := child(t, ds, aaa, "aaaa")
	aaaaa := child(t, ds, aaaa, "aaaaa")
	addLink(t, ds, aaaa, aaaaa)
	addLink(t, ds, aaaa, aaaaa)
	addLink(t, ds, aaa, aaaa)
	addLink(t, ds, aaa, aaaa)
	addLink(t, ds, aa, aaa)
	addLink(t, ds, aa, aaa)
	addLink(t, ds, a, aa)
	addLink(t, ds, a, aa)
	return a
}

func addLink(t *testing.T, ds ipld.DAGService, a, b ipld.Node) {
	to := string(a.(*mdag.ProtoNode).Data()) + "2" + string(b.(*mdag.ProtoNode).Data())
	if err := ds.Add(context.Background(), b); err != nil {
		t.Error(err)
	}
	if err := a.(*mdag.ProtoNode).AddNodeLink(to, b.(*mdag.ProtoNode)); err != nil {
		t.Error(err)
	}
}

func child(t *testing.T, ds ipld.DAGService, a ipld.Node, name string) ipld.Node {
	return mdag.NodeWithData([]byte(string(a.(*mdag.ProtoNode).Data()) + "/" + name))
}
