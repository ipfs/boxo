package dagutils

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"

	dag "github.com/ipfs/boxo/ipld/merkledag"
	mdtest "github.com/ipfs/boxo/ipld/merkledag/test"
)

func buildNode(name string, desc map[string]ndesc, out map[string]ipld.Node) ipld.Node {
	this := desc[name]
	nd := new(dag.ProtoNode)
	nd.SetData([]byte(name))
	for k, v := range this {
		child, ok := out[v]
		if !ok {
			child = buildNode(v, desc, out)
			out[v] = child
		}

		if err := nd.AddNodeLink(k, child); err != nil {
			panic(err)
		}
	}

	return nd
}

type ndesc map[string]string

func mkGraph(desc map[string]ndesc) map[string]ipld.Node {
	out := make(map[string]ipld.Node)
	for name := range desc {
		if _, ok := out[name]; ok {
			continue
		}

		out[name] = buildNode(name, desc, out)
	}
	return out
}

var tg1 = map[string]ndesc{
	"a1": {
		"foo": "b",
	},
	"b": {},
	"a2": {
		"foo": "b",
		"bar": "c",
	},
	"c": {},
}

var tg2 = map[string]ndesc{
	"a1": {
		"foo": "b",
	},
	"b": {},
	"a2": {
		"foo": "b",
		"bar": "c",
	},
	"c": {"baz": "d"},
	"d": {},
}

var tg3 = map[string]ndesc{
	"a1": {
		"foo": "b",
		"bar": "c",
	},
	"b": {},
	"a2": {
		"foo": "b",
		"bar": "d",
	},
	"c": {},
	"d": {},
}

var tg4 = map[string]ndesc{
	"a1": {
		"key1": "b",
		"key2": "c",
	},
	"a2": {
		"key1": "b",
		"key2": "d",
	},
}

var tg5 = map[string]ndesc{
	"a1": {
		"key1": "a",
		"key2": "b",
	},
	"a2": {
		"key1": "c",
		"key2": "d",
	},
}

func TestNameMatching(t *testing.T) {
	nds := mkGraph(tg4)

	diff := getLinkDiff(nds["a1"], nds["a2"])
	if len(diff) != 1 {
		t.Fatal(errors.New("node diff didn't match by name"))
	}
}

func TestNameMatching2(t *testing.T) {
	nds := mkGraph(tg5)

	diff := getLinkDiff(nds["a1"], nds["a2"])
	if len(diff) != 2 {
		t.Fatal(errors.New("incorrect number of link diff elements"))
	}
	if !(diff[0].bef.Equals(nds["a1"].Links()[0].Cid) && diff[0].aft.Equals(nds["a2"].Links()[0].Cid)) {
		t.Fatal(errors.New("node diff didn't match by name"))
	}
}

func TestDiffEnumBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nds := mkGraph(tg1)

	ds := mdtest.Mock()
	lgds := &getLogger{ds: ds}

	for _, nd := range nds {
		err := ds.Add(ctx, nd)
		if err != nil {
			t.Fatal(err)
		}
	}

	err := DiffEnumerate(ctx, lgds, nds["a1"].Cid(), nds["a2"].Cid())
	if err != nil {
		t.Fatal(err)
	}

	err = assertCidList(lgds.log, []cid.Cid{nds["a1"].Cid(), nds["a2"].Cid(), nds["c"].Cid()})
	if err != nil {
		t.Fatal(err)
	}
}

type getLogger struct {
	ds  ipld.NodeGetter
	log []cid.Cid
}

func (gl *getLogger) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	nd, err := gl.ds.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	gl.log = append(gl.log, c)
	return nd, nil
}

func (gl *getLogger) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipld.NodeOption {
	outCh := make(chan *ipld.NodeOption, len(cids))
	nds := gl.ds.GetMany(ctx, cids)
	for no := range nds {
		if no.Err == nil {
			gl.log = append(gl.log, no.Node.Cid())
		}
		select {
		case outCh <- no:
		default:
			panic("too many responses")
		}
	}
	return nds
}

func assertCidList(a, b []cid.Cid) error {
	if len(a) != len(b) {
		return errors.New("got different number of cids than expected")
	}
	for i, c := range a {
		if !c.Equals(b[i]) {
			return fmt.Errorf("expected %s, got %s", c, b[i])
		}
	}
	return nil
}

func TestDiffEnumFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nds := mkGraph(tg2)

	ds := mdtest.Mock()
	lgds := &getLogger{ds: ds}

	for _, s := range []string{"a1", "a2", "b", "c"} {
		err := ds.Add(ctx, nds[s])
		if err != nil {
			t.Fatal(err)
		}
	}

	err := DiffEnumerate(ctx, lgds, nds["a1"].Cid(), nds["a2"].Cid())
	if !ipld.IsNotFound(err) {
		t.Fatal("expected err not found")
	}

	err = assertCidList(lgds.log, []cid.Cid{nds["a1"].Cid(), nds["a2"].Cid(), nds["c"].Cid()})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDiffEnumRecurse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nds := mkGraph(tg3)

	ds := mdtest.Mock()
	lgds := &getLogger{ds: ds}

	for _, s := range []string{"a1", "a2", "b", "c", "d"} {
		err := ds.Add(ctx, nds[s])
		if err != nil {
			t.Fatal(err)
		}
	}

	err := DiffEnumerate(ctx, lgds, nds["a1"].Cid(), nds["a2"].Cid())
	if err != nil {
		t.Fatal(err)
	}

	err = assertCidList(lgds.log, []cid.Cid{nds["a1"].Cid(), nds["a2"].Cid(), nds["c"].Cid(), nds["d"].Cid()})
	if err != nil {
		t.Fatal(err)
	}
}
