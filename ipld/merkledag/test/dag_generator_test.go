package mdutils

import (
	"context"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
)

type testDagServ struct {
	mu    sync.Mutex
	nodes map[string]format.Node
}

func newTestDagServ() *testDagServ {
	return &testDagServ{nodes: make(map[string]format.Node)}
}

func (d *testDagServ) Get(_ context.Context, cid cid.Cid) (format.Node, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if n, ok := d.nodes[cid.KeyString()]; ok {
		return n, nil
	}
	return nil, format.ErrNotFound{Cid: cid}
}

func (d *testDagServ) Add(_ context.Context, node format.Node) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nodes[node.Cid().KeyString()] = node
	return nil
}

func TestNodesAreDifferent(t *testing.T) {
	dserv := newTestDagServ()
	gen := NewDAGGenerator()

	var allCids []cid.Cid
	var allNodes []format.Node

	const nbDag = 5

	for i := 0; i < nbDag; i++ {
		c, cids, err := gen.MakeDagNode(dserv.Add, 5, 3)
		if err != nil {
			t.Fatal(err)
		}

		allCids = append(allCids, cids...)

		// collect all nodes
		var getChildren func(n format.Node)
		getChildren = func(n format.Node) {
			for _, link := range n.Links() {
				n, err = dserv.Get(context.Background(), link.Cid)
				if err != nil {
					t.Fatal(err)
				}
				allNodes = append(allNodes, n)
				getChildren(n)
			}
		}
		n, err := dserv.Get(context.Background(), c)
		if err != nil {
			t.Fatal(err)
		}
		allNodes = append(allNodes, n)
		getChildren(n)

		// make sure they are all different
		for i, node1 := range allNodes {
			for j, node2 := range allNodes {
				if i != j {
					if node1.Cid().String() == node2.Cid().String() {
						t.Error("Found duplicate node")
					}
				}
			}
		}
	}

	// expected count
	if len(allNodes) != nbDag*31 {
		t.Error("expected nbDag*31 nodes (1+5+5*5)")
	}
	if len(allCids) != nbDag*31 {
		t.Error("expected nbDag*31 cids (1+5+5*5)")
	}
}
