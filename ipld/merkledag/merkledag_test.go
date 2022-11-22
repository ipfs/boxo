package merkledag_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/ipfs/go-merkledag"
	mdpb "github.com/ipfs/go-merkledag/pb"
	dstest "github.com/ipfs/go-merkledag/test"

	blocks "github.com/ipfs/go-block-format"
	bserv "github.com/ipfs/go-blockservice"
	bstest "github.com/ipfs/go-blockservice/test"
	cid "github.com/ipfs/go-cid"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	u "github.com/ipfs/go-ipfs-util"
	ipld "github.com/ipfs/go-ipld-format"
	prime "github.com/ipld/go-ipld-prime"
	mh "github.com/multiformats/go-multihash"
)

var someCid cid.Cid = func() cid.Cid {
	c, _ := cid.Cast([]byte{1, 85, 0, 5, 0, 1, 2, 3, 4})
	return c
}()

// makeDepthTestingGraph makes a small DAG with two levels. The level-two
// nodes are both children of the root and of one of the level 1 nodes.
// This is meant to test the Walk*Depth functions.
func makeDepthTestingGraph(t *testing.T, ds ipld.DAGService) ipld.Node {
	root := NodeWithData(nil)
	l11 := NodeWithData([]byte("leve1_node1"))
	l12 := NodeWithData([]byte("leve1_node2"))
	l21 := NodeWithData([]byte("leve2_node1"))
	l22 := NodeWithData([]byte("leve2_node2"))
	l23 := NodeWithData([]byte("leve2_node3"))

	l11.AddNodeLink(l21.Cid().String(), l21)
	l11.AddNodeLink(l22.Cid().String(), l22)
	l11.AddNodeLink(l23.Cid().String(), l23)

	root.AddNodeLink(l11.Cid().String(), l11)
	root.AddNodeLink(l12.Cid().String(), l12)
	root.AddNodeLink(l23.Cid().String(), l23)

	ctx := context.Background()
	for _, n := range []ipld.Node{l23, l22, l21, l12, l11, root} {
		err := ds.Add(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
	}

	return root
}

// Check that all children of root are in the given set and in the datastore
func traverseAndCheck(t *testing.T, root ipld.Node, ds ipld.DAGService, hasF func(c cid.Cid) bool) {
	// traverse dag and check
	for _, lnk := range root.Links() {
		c := lnk.Cid
		if !hasF(c) {
			t.Fatal("missing key in set! ", lnk.Cid.String())
		}
		child, err := ds.Get(context.Background(), c)
		if err != nil {
			t.Fatal(err)
		}
		traverseAndCheck(t, child, ds, hasF)
	}
}

type brokenBuilder struct{}

func (brokenBuilder) Sum([]byte) (cid.Cid, error)    { return cid.Undef, errors.New("Nope!") }
func (brokenBuilder) GetCodec() uint64               { return 0 }
func (b brokenBuilder) WithCodec(uint64) cid.Builder { return b }

// builder that will pass the basic SetCidBuilder tests but fail otherwise
type sneakyBrokenBuilder struct{}

func (sneakyBrokenBuilder) Sum(data []byte) (cid.Cid, error) {
	if len(data) == 256 {
		return V1CidPrefix().Sum(data)
	}
	return cid.Undef, errors.New("Nope!")
}
func (sneakyBrokenBuilder) GetCodec() uint64               { return 0 }
func (b sneakyBrokenBuilder) WithCodec(uint64) cid.Builder { return b }

func TestBadBuilderEncode(t *testing.T) {
	n := NodeWithData([]byte("boop"))

	t.Run("good builder sanity check", func(t *testing.T) {
		if _, err := n.EncodeProtobuf(false); err != nil {
			t.Fatal(err)
		}
		if err := n.SetCidBuilder(
			&cid.Prefix{
				MhType:   mh.SHA2_256,
				MhLength: -1,
				Version:  1,
				Codec:    cid.DagProtobuf,
			},
		); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("hasher we can't use, should error", func(t *testing.T) {
		if err := n.SetCidBuilder(
			&cid.Prefix{
				MhType:   mh.SHA2_256_TRUNC254_PADDED,
				MhLength: 256,
				Version:  1,
				Codec:    cid.DagProtobuf,
			},
		); err == nil {
			t.Fatal("expected SetCidBuilder to error on unusable hasher")
		}
		if _, err := n.EncodeProtobuf(false); err != nil {
			t.Fatalf("expected EncodeProtobuf to use safe CidBuilder: %v", err)
		}
	})

	t.Run("broken custom builder, should error", func(t *testing.T) {
		if err := n.SetCidBuilder(brokenBuilder{}); err == nil {
			t.Fatal("expected SetCidBuilder to error on unusable hasher")
		}
		if _, err := n.EncodeProtobuf(false); err != nil {
			t.Fatalf("expected EncodeProtobuf to use safe CidBuilder: %v", err)
		}
	})

	t.Run("broken custom builder as pointer, should error", func(t *testing.T) {
		if err := n.SetCidBuilder(&brokenBuilder{}); err == nil {
			t.Fatal("expected SetCidBuilder to error on unusable hasher")
		}
		if _, err := n.EncodeProtobuf(false); err != nil {
			t.Fatalf("expected EncodeProtobuf to use safe CidBuilder: %v", err)
		}
	})

	t.Run("broken sneaky custom builder, should error", func(t *testing.T) {
		if err := n.SetCidBuilder(sneakyBrokenBuilder{}); err != nil {
			t.Fatalf("expected SetCidBuilder to not error with sneaky custom builder: %v", err)
		}
		if _, err := n.EncodeProtobuf(false); err == nil {
			t.Fatal("expected EncodeProtobuf to fail using the sneaky custom builder")
		}
		if len(n.RawData()) != 0 {
			t.Fatal("expected RawData to return zero-byte slice")
		}
		if n.Cid().String() != "bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku" {
			t.Fatal("expected Cid to return the zero dag-pb CID")
		}
		if n.String() != "bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku" {
			t.Fatal("expected String to return the zero dag-pb CID string")
		}
	})
}

func TestLinkChecking(t *testing.T) {
	cases := []struct {
		name string
		fn   func(*ProtoNode) error
	}{
		{
			name: "AddRawLink overflow Tsize",
			fn: func(n *ProtoNode) error {
				return n.AddRawLink("foo", &ipld.Link{Size: math.MaxUint64, Cid: someCid})
			},
		},

		{
			name: "AddRawLink undefined CID",
			fn: func(n *ProtoNode) error {
				return n.AddRawLink("foo", &ipld.Link{Cid: cid.Undef})
			},
		},

		{
			name: "SetLinks overflow Tsize",
			fn: func(n *ProtoNode) error {
				return n.SetLinks([]*ipld.Link{{Size: math.MaxUint64, Cid: someCid}})
			},
		},

		{
			name: "SetLinks undefined CID",
			fn: func(n *ProtoNode) error {
				return n.SetLinks([]*ipld.Link{{Cid: cid.Undef}})
			},
		},

		{
			name: "UnmarshalJSON overflow Tsize",
			fn: func(n *ProtoNode) error {
				return n.UnmarshalJSON([]byte(`{"data":null,"links":[{"Name":"","Size":18446744073709549568,"Cid":{"/":"QmNPWHBrVQiiV8FpyNuEPhB9E2rbvdy9Yx79EY1EJuyf9o"}}]}`))
			},
		},

		{
			name: "UnmarshalJSON undefined CID",
			fn: func(n *ProtoNode) error {
				return n.UnmarshalJSON([]byte(`{"data":null,"links":[{"Name":"","Size":100}]}`))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := NodeWithData([]byte("boop"))
			err := tc.fn(n)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}

	t.Run("round-trip block with bad Tsize", func(t *testing.T) {
		badblock, _ := hex.DecodeString("122f0a22122000bb3604d2ecd386227007c548249521fbb9a394e1e26460091d0a692888e7361880f0ffffffffffffff01")
		n, err := DecodeProtobuf(badblock)
		if err != nil {
			t.Fatal(err)
		}
		// sanity
		if len(n.Links()) != 1 {
			t.Fatal("expected a link")
		}
		// sanity
		if n.Links()[0].Size <= math.MaxInt64 {
			t.Fatal("expected link Tsize to be oversized")
		}

		// forced round-trip
		byts, err := n.EncodeProtobuf(true)
		if err != nil {
			t.Fatal(err)
		}
		n, err = DecodeProtobuf(byts)
		if err != nil {
			t.Fatal(err)
		}
		if len(n.Links()) != 1 {
			t.Fatal("expected a link")
		}
		if n.Links()[0].Size != 0 {
			t.Fatal("expected link Tsize to be truncated on reencode")
		}
	})
}

func TestNode(t *testing.T) {

	n1 := NodeWithData([]byte("beep"))
	n2 := NodeWithData([]byte("boop"))
	n3 := NodeWithData([]byte("beep boop"))
	if err := n3.AddNodeLink("beep-link", n1); err != nil {
		t.Error(err)
	}
	if err := n3.AddNodeLink("boop-link", n2); err != nil {
		t.Error(err)
	}

	printn := func(name string, n *ProtoNode) {
		fmt.Println(">", name)
		fmt.Println("data:", string(n.Data()))

		fmt.Println("links:")
		for _, l := range n.Links() {
			fmt.Println("-", l.Name, l.Size, l.Cid)
		}

		e, err := n.EncodeProtobuf(false)
		if err != nil {
			t.Error(err)
		} else {
			fmt.Println("encoded:", e)
		}

		h := n.Multihash()
		k := n.Cid().Hash()
		if k.String() != h.String() {
			t.Error("Key is not equivalent to multihash")
		} else {
			fmt.Println("key: ", k)
		}

		SubtestNodeStat(t, n)
	}

	printn("beep", n1)
	printn("boop", n2)
	printn("beep boop", n3)
}

func SubtestNodeStat(t *testing.T, n *ProtoNode) {
	enc, err := n.EncodeProtobuf(true)
	if err != nil {
		t.Error("n.EncodeProtobuf(true) failed")
		return
	}

	cumSize, err := n.Size()
	if err != nil {
		t.Error("n.Size() failed")
		return
	}

	k := n.Cid()

	expected := ipld.NodeStat{
		NumLinks:       len(n.Links()),
		BlockSize:      len(enc),
		LinksSize:      len(enc) - len(n.Data()), // includes framing.
		DataSize:       len(n.Data()),
		CumulativeSize: int(cumSize),
		Hash:           k.String(),
	}

	actual, err := n.Stat()
	if err != nil {
		t.Error("n.Stat() failed")
		return
	}

	if expected != *actual {
		t.Errorf("n.Stat incorrect.\nexpect: %s\nactual: %s", expected, actual)
	} else {
		fmt.Printf("n.Stat correct: %s\n", actual)
	}
}

type devZero struct{}

func (devZero) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = 0
	}
	return len(b), nil
}

func TestBatchFetch(t *testing.T) {
	read := io.LimitReader(u.NewTimeSeededRand(), 1024*32)
	runBatchFetchTest(t, read)
}

func TestBatchFetchDupBlock(t *testing.T) {
	read := io.LimitReader(devZero{}, 1024*32)
	runBatchFetchTest(t, read)
}

// makeTestDAG creates a simple DAG from the data in a reader.
// First, a node is created from each 512 bytes of data from the reader
// (like a the Size chunker would do). Then all nodes are added as children
// to a root node, which is returned.
func makeTestDAG(t *testing.T, read io.Reader, ds ipld.DAGService) ipld.Node {
	p := make([]byte, 512)
	nodes := []*ProtoNode{}

	for {
		n, err := io.ReadFull(read, p)
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		if n != len(p) {
			t.Fatal("should have read 512 bytes from the reader")
		}

		protoNode := NodeWithData(p)
		nodes = append(nodes, protoNode)
	}

	ctx := context.Background()
	// Add a root referencing all created nodes
	root := NodeWithData(nil)
	for _, n := range nodes {
		err := root.AddNodeLink(n.Cid().String(), n)
		if err != nil {
			t.Fatal(err)
		}
		err = ds.Add(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
	}
	err := ds.Add(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// makeTestDAGReader takes the root node as returned by makeTestDAG and
// provides a reader that reads all the RawData from that node and its children.
func makeTestDAGReader(t *testing.T, root ipld.Node, ds ipld.DAGService) io.Reader {
	ctx := context.Background()
	buf := new(bytes.Buffer)
	buf.Write(root.RawData())
	for _, l := range root.Links() {
		n, err := ds.Get(ctx, l.Cid)
		if err != nil {
			t.Fatal(err)
		}
		_, err = buf.Write(n.RawData())
		if err != nil {
			t.Fatal(err)
		}
	}
	return buf
}

func runBatchFetchTest(t *testing.T, read io.Reader) {
	ctx := context.Background()
	var dagservs []ipld.DAGService
	for _, bsi := range bstest.Mocks(5) {
		dagservs = append(dagservs, NewDAGService(bsi))
	}

	root := makeTestDAG(t, read, dagservs[0])

	t.Log("finished setup.")

	dagr := makeTestDAGReader(t, root, dagservs[0])

	expected, err := io.ReadAll(dagr)
	if err != nil {
		t.Fatal(err)
	}

	err = dagservs[0].Add(ctx, root)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Added file to first node.")

	c := root.Cid()

	wg := sync.WaitGroup{}
	errs := make(chan error)

	for i := 1; i < len(dagservs); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			first, err := dagservs[i].Get(ctx, c)
			if err != nil {
				errs <- err
			}
			fmt.Println("Got first node back.")

			firstpb, ok := first.(*ProtoNode)
			if !ok {
				errs <- ErrNotProtobuf
			}
			read := makeTestDAGReader(t, firstpb, dagservs[i])
			datagot, err := io.ReadAll(read)
			if err != nil {
				errs <- err
			}

			if !bytes.Equal(datagot, expected) {
				errs <- errors.New("got bad data back")
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(errs)
	}()

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCantGet(t *testing.T) {
	ds := dstest.Mock()
	a := NodeWithData([]byte("A"))

	c := a.Cid()
	_, err := ds.Get(context.Background(), c)
	if !strings.Contains(err.Error(), "not found") {
		t.Fatal("expected err not found, got: ", err)
	}
}

func TestFetchGraph(t *testing.T) {
	var dservs []ipld.DAGService
	bsis := bstest.Mocks(2)
	for _, bsi := range bsis {
		dservs = append(dservs, NewDAGService(bsi))
	}

	read := io.LimitReader(u.NewTimeSeededRand(), 1024*32)
	root := makeTestDAG(t, read, dservs[0])

	err := FetchGraph(context.TODO(), root.Cid(), dservs[1])
	if err != nil {
		t.Fatal(err)
	}

	// create an offline dagstore and ensure all blocks were fetched
	bs := bserv.New(bsis[1].Blockstore(), offline.Exchange(bsis[1].Blockstore()))

	offlineDS := NewDAGService(bs)

	err = Walk(context.Background(), offlineDS.GetLinks, root.Cid(), func(_ cid.Cid) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
}

func TestFetchGraphWithDepthLimit(t *testing.T) {
	type testcase struct {
		depthLim int
		setLen   int
	}

	tests := []testcase{
		{1, 4},
		{0, 1},
		{-1, 6},
		{2, 6},
		{3, 6},
	}

	testF := func(t *testing.T, tc testcase) {
		var dservs []ipld.DAGService
		bsis := bstest.Mocks(2)
		for _, bsi := range bsis {
			dservs = append(dservs, NewDAGService(bsi))
		}

		root := makeDepthTestingGraph(t, dservs[0])

		err := FetchGraphWithDepthLimit(context.TODO(), root.Cid(), tc.depthLim, dservs[1])
		if err != nil {
			t.Fatal(err)
		}

		// create an offline dagstore and ensure all blocks were fetched
		bs := bserv.New(bsis[1].Blockstore(), offline.Exchange(bsis[1].Blockstore()))

		offlineDS := NewDAGService(bs)

		set := make(map[string]int)
		visitF := func(c cid.Cid, depth int) bool {
			if tc.depthLim < 0 || depth <= tc.depthLim {
				set[string(c.Bytes())] = depth
				return true
			}
			return false

		}

		err = WalkDepth(context.Background(), offlineDS.GetLinks, root.Cid(), visitF)
		if err != nil {
			t.Fatal(err)
		}

		if len(set) != tc.setLen {
			t.Fatalf("expected %d nodes but visited %d", tc.setLen, len(set))
		}
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("depth limit %d", tc.depthLim), func(t *testing.T) {
			testF(t, tc)
		})
	}
}

func TestWalk(t *testing.T) {
	bsi := bstest.Mocks(1)
	ds := NewDAGService(bsi[0])

	read := io.LimitReader(u.NewTimeSeededRand(), 1024*1024)
	root := makeTestDAG(t, read, ds)

	set := cid.NewSet()

	err := Walk(context.Background(), ds.GetLinks, root.Cid(), set.Visit)
	if err != nil {
		t.Fatal(err)
	}

	traverseAndCheck(t, root, ds, set.Has)
}

func TestFetchFailure(t *testing.T) {
	ctx := context.Background()

	ds := dstest.Mock()
	ds_bad := dstest.Mock()

	top := new(ProtoNode)
	for i := 0; i < 10; i++ {
		nd := NodeWithData([]byte{byte('a' + i)})
		err := ds.Add(ctx, nd)
		if err != nil {
			t.Fatal(err)
		}

		err = top.AddNodeLink(fmt.Sprintf("AA%d", i), nd)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 10; i++ {
		nd := NodeWithData([]byte{'f', 'a' + byte(i)})
		err := ds_bad.Add(ctx, nd)
		if err != nil {
			t.Fatal(err)
		}

		err = top.AddNodeLink(fmt.Sprintf("BB%d", i), nd)
		if err != nil {
			t.Fatal(err)
		}
	}

	getters := ipld.GetDAG(ctx, ds, top)
	for i, getter := range getters {
		_, err := getter.Get(ctx)
		if err != nil && i < 10 {
			t.Fatal(err)
		}
		if err == nil && i >= 10 {
			t.Fatal("should have failed request")
		}
	}
}

func TestUnmarshalFailure(t *testing.T) {
	badData := []byte("hello world")

	_, err := DecodeProtobuf(badData)
	if err == nil {
		t.Fatal("shouldnt succeed to parse this")
	}

	// now with a bad link
	pbn := &mdpb.PBNode{Links: []*mdpb.PBLink{{Hash: []byte("not a multihash")}}}
	badlink, err := pbn.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	_, err = DecodeProtobuf(badlink)
	if err == nil {
		t.Fatal("should have failed to parse node with bad link")
	}

	n := &ProtoNode{}
	n.Marshal()
}

func TestBasicAddGet(t *testing.T) {
	ctx := context.Background()

	ds := dstest.Mock()
	nd := new(ProtoNode)

	err := ds.Add(ctx, nd)
	if err != nil {
		t.Fatal(err)
	}

	out, err := ds.Get(ctx, nd.Cid())
	if err != nil {
		t.Fatal(err)
	}

	if !nd.Cid().Equals(out.Cid()) {
		t.Fatal("output didnt match input")
	}
}

func TestGetRawNodes(t *testing.T) {
	ctx := context.Background()

	rn := NewRawNode([]byte("test"))

	ds := dstest.Mock()

	err := ds.Add(ctx, rn)
	if err != nil {
		t.Fatal(err)
	}

	if !rn.Cid().Equals(rn.Cid()) {
		t.Fatal("output cids didnt match")
	}

	out, err := ds.Get(ctx, rn.Cid())
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(out.RawData(), []byte("test")) {
		t.Fatal("raw block should match input data")
	}

	if out.Links() != nil {
		t.Fatal("raw blocks shouldnt have links")
	}

	if out.Tree("", -1) != nil {
		t.Fatal("tree should return no paths in a raw block")
	}

	size, err := out.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != 4 {
		t.Fatal("expected size to be 4")
	}

	ns, err := out.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if ns.DataSize != 4 {
		t.Fatal("expected size to be 4, got: ", ns.DataSize)
	}

	_, _, err = out.Resolve([]string{"foo"})
	if err != ErrLinkNotFound {
		t.Fatal("shouldnt find links under raw blocks")
	}
}

func TestProtoNodeResolve(t *testing.T) {

	nd := new(ProtoNode)
	nd.SetLinks([]*ipld.Link{{Name: "foo", Cid: someCid}})

	lnk, left, err := nd.ResolveLink([]string{"foo", "bar"})
	if err != nil {
		t.Fatal(err)
	}

	if len(left) != 1 || left[0] != "bar" {
		t.Fatal("expected the single path element 'bar' to remain")
	}

	if lnk.Name != "foo" {
		t.Fatal("how did we get anything else?")
	}

	tvals := nd.Tree("", -1)
	if len(tvals) != 1 || tvals[0] != "foo" {
		t.Fatal("expected tree to return []{\"foo\"}")
	}
}

func TestCidRetention(t *testing.T) {
	ctx := context.Background()

	nd := new(ProtoNode)
	nd.SetData([]byte("fooooo"))

	pref := nd.Cid().Prefix()
	pref.Version = 1

	c2, err := pref.Sum(nd.RawData())
	if err != nil {
		t.Fatal(err)
	}

	blk, err := blocks.NewBlockWithCid(nd.RawData(), c2)
	if err != nil {
		t.Fatal(err)
	}

	bs := dstest.Bserv()
	err = bs.AddBlock(ctx, blk)
	if err != nil {
		t.Fatal(err)
	}

	ds := NewDAGService(bs)
	out, err := ds.Get(ctx, c2)
	if err != nil {
		t.Fatal(err)
	}

	if !out.Cid().Equals(c2) {
		t.Fatal("output cid didnt match")
	}
}

func TestCidRawDoesnNeedData(t *testing.T) {
	srv := NewDAGService(dstest.Bserv())
	nd := NewRawNode([]byte("somedata"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// there is no data for this node in the blockservice
	// so dag service can't load it
	links, err := srv.GetLinks(ctx, nd.Cid())
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 0 {
		t.Fatal("raw node shouldn't have any links")
	}
}

func TestRawToJson(t *testing.T) {
	rawData := []byte{1, 2, 3, 4}
	nd := NewRawNode(rawData)
	encoded, err := nd.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var res interface{}
	err = json.Unmarshal(encoded, &res)
	if err != nil {
		t.Fatal(err)
	}
	resBytes, ok := res.(string)
	if !ok {
		t.Fatal("expected to marshal to a string")
	}
	if string(rawData) != resBytes {
		t.Fatal("failed to round-trip bytes")
	}
}

func TestGetManyDuplicate(t *testing.T) {
	ctx := context.Background()

	srv := NewDAGService(dstest.Bserv())

	nd := NodeWithData([]byte("foo"))
	if err := srv.Add(ctx, nd); err != nil {
		t.Fatal(err)
	}
	nds := srv.GetMany(ctx, []cid.Cid{nd.Cid(), nd.Cid(), nd.Cid()})
	out, ok := <-nds
	if !ok {
		t.Fatal("expecting node foo")
	}
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	if !out.Node.Cid().Equals(nd.Cid()) {
		t.Fatal("got wrong node")
	}
	out, ok = <-nds
	if ok {
		if out.Err != nil {
			t.Fatal(out.Err)
		} else {
			t.Fatal("expecting no more nodes")
		}
	}
}

func TestEnumerateAsyncFailsNotFound(t *testing.T) {
	ctx := context.Background()

	a := NodeWithData([]byte("foo1"))
	b := NodeWithData([]byte("foo2"))
	c := NodeWithData([]byte("foo3"))
	d := NodeWithData([]byte("foo4"))
	e := NodeWithData([]byte("foo5"))

	ds := dstest.Mock()
	for _, n := range []ipld.Node{a, b, c} {
		err := ds.Add(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
	}

	parent := new(ProtoNode)
	if err := parent.AddNodeLink("a", a); err != nil {
		t.Fatal(err)
	}

	if err := parent.AddNodeLink("b", b); err != nil {
		t.Fatal(err)
	}

	if err := parent.AddNodeLink("c", c); err != nil {
		t.Fatal(err)
	}

	if err := parent.AddNodeLink("d", d); err != nil {
		t.Fatal(err)
	}

	if err := parent.AddNodeLink("e", e); err != nil {
		t.Fatal(err)
	}

	err := ds.Add(ctx, parent)
	if err != nil {
		t.Fatal(err)
	}

	cset := cid.NewSet()
	err = Walk(ctx, GetLinksDirect(ds), parent.Cid(), cset.Visit)
	if err == nil {
		t.Fatal("this should have failed")
	}
}

func TestLinkSorting(t *testing.T) {
	az := "az"
	aaaa := "aaaa"
	bbbb := "bbbb"
	cccc := "cccc"

	azBlk := NewRawNode([]byte(az))
	aaaaBlk := NewRawNode([]byte(aaaa))
	bbbbBlk := NewRawNode([]byte(bbbb))
	ccccBlk := NewRawNode([]byte(cccc))
	pbn := &mdpb.PBNode{
		Links: []*mdpb.PBLink{
			{Hash: bbbbBlk.Cid().Bytes(), Name: &bbbb},
			{Hash: azBlk.Cid().Bytes(), Name: &az},
			{Hash: aaaaBlk.Cid().Bytes(), Name: &aaaa},
			{Hash: ccccBlk.Cid().Bytes(), Name: &cccc},
		},
	}
	byts, err := pbn.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	mustLookupNodeString := func(t *testing.T, node prime.Node, name string) prime.Node {
		subNode, err := node.LookupByString(name)
		if err != nil {
			t.Fatal(err)
		}
		return subNode
	}

	mustLookupNodeIndex := func(t *testing.T, node prime.Node, idx int64) prime.Node {
		subNode, err := node.LookupByIndex(idx)
		if err != nil {
			t.Fatal(err)
		}
		return subNode
	}

	mustNodeAsString := func(t *testing.T, node prime.Node) string {
		str, err := node.AsString()
		if err != nil {
			t.Fatal(err)
		}
		return str
	}

	verifyUnsortedNode := func(t *testing.T, node *ProtoNode) {
		links := node.Links()
		if len(links) != 4 {
			t.Errorf("wrong number of links, expected 4 but got %d", len(links))
		}
		if links[0].Name != bbbb {
			t.Errorf("expected link 0 to be 'bbbb', got %s", links[0].Name)
		}
		if links[1].Name != az {
			t.Errorf("expected link 0 to be 'az', got %s", links[1].Name)
		}
		if links[2].Name != aaaa {
			t.Errorf("expected link 0 to be 'aaaa', got %s", links[2].Name)
		}
		if links[3].Name != cccc {
			t.Errorf("expected link 0 to be 'cccc', got %s", links[3].Name)
		}

		// check the go-ipld-prime form
		linksNode := mustLookupNodeString(t, node, "Links")
		if linksNode.Length() != 4 {
			t.Errorf("(Node) wrong number of links, expected 4 but got %d", len(links))
		}
		if name := mustNodeAsString(t, mustLookupNodeString(t, mustLookupNodeIndex(t, linksNode, 0), "Name")); name != bbbb {
			t.Errorf("(Node) expected link 0 to be 'bbbb', got %s", name)
		}
		if name := mustNodeAsString(t, mustLookupNodeString(t, mustLookupNodeIndex(t, linksNode, 1), "Name")); name != az {
			t.Errorf("(Node) expected link 0 to be 'az', got %s", name)
		}
		if name := mustNodeAsString(t, mustLookupNodeString(t, mustLookupNodeIndex(t, linksNode, 2), "Name")); name != aaaa {
			t.Errorf("(Node) expected link 0 to be 'aaaa', got %s", name)
		}
		if name := mustNodeAsString(t, mustLookupNodeString(t, mustLookupNodeIndex(t, linksNode, 3), "Name")); name != cccc {
			t.Errorf("(Node) expected link 0 to be 'cccc', got %s", name)
		}
	}

	verifySortedNode := func(t *testing.T, node *ProtoNode) {
		links := node.Links()
		if len(links) != 4 {
			t.Errorf("wrong number of links, expected 4 but got %d", len(links))
		}
		if links[0].Name != aaaa {
			t.Errorf("expected link 0 to be 'aaaa', got %s", links[0].Name)
		}
		if links[1].Name != az {
			t.Errorf("expected link 0 to be 'az', got %s", links[1].Name)
		}
		if links[2].Name != bbbb {
			t.Errorf("expected link 0 to be 'bbbb', got %s", links[2].Name)
		}
		if links[3].Name != cccc {
			t.Errorf("expected link 0 to be 'cccc', got %s", links[3].Name)
		}

		// check the go-ipld-prime form
		linksNode := mustLookupNodeString(t, node, "Links")
		if linksNode.Length() != 4 {
			t.Errorf("(Node) wrong number of links, expected 4 but got %d", len(links))
		}
		if name := mustNodeAsString(t, mustLookupNodeString(t, mustLookupNodeIndex(t, linksNode, 0), "Name")); name != aaaa {
			t.Errorf("(Node) expected link 0 to be 'aaaa', got %s", name)
		}
		if name := mustNodeAsString(t, mustLookupNodeString(t, mustLookupNodeIndex(t, linksNode, 1), "Name")); name != az {
			t.Errorf("(Node) expected link 0 to be 'az', got %s", name)
		}
		if name := mustNodeAsString(t, mustLookupNodeString(t, mustLookupNodeIndex(t, linksNode, 2), "Name")); name != bbbb {
			t.Errorf("(Node) expected link 0 to be 'bbbb', got %s", name)
		}
		if name := mustNodeAsString(t, mustLookupNodeString(t, mustLookupNodeIndex(t, linksNode, 3), "Name")); name != cccc {
			t.Errorf("(Node) expected link 0 to be 'cccc', got %s", name)
		}
	}

	t.Run("decode", func(t *testing.T) {
		node, err := DecodeProtobuf(byts)
		if err != nil {
			t.Fatal(err)
		}
		verifyUnsortedNode(t, node)
	})

	t.Run("RawData() should not mutate, should return original form", func(t *testing.T) {
		node, err := DecodeProtobuf(byts)
		if err != nil {
			t.Fatal(err)
		}
		rawData := node.RawData()
		verifyUnsortedNode(t, node)
		if !bytes.Equal(rawData, byts) {
			t.Error("RawData() did not return original bytes")
		}
	})

	t.Run("Size() should not mutate", func(t *testing.T) {
		node, err := DecodeProtobuf(byts)
		if err != nil {
			t.Fatal(err)
		}
		sz, err := node.Size()
		if err != nil {
			t.Fatal(err)
		}
		if sz != 182 {
			t.Errorf("expected size to be 182, got %d", sz)
		}
		verifyUnsortedNode(t, node)
	})

	t.Run("GetPBNode() should not mutate, returned PBNode should be sorted", func(t *testing.T) {
		node, err := DecodeProtobuf(byts)
		if err != nil {
			t.Fatal(err)
		}
		rtPBNode := node.GetPBNode()
		rtByts, err := rtPBNode.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		verifyUnsortedNode(t, node)
		rtNode, err := DecodeProtobuf(rtByts)
		if err != nil {
			t.Fatal(err)
		}
		verifySortedNode(t, rtNode)
	})

	t.Run("add and remove link should mutate", func(t *testing.T) {
		node, err := DecodeProtobuf(byts)
		if err != nil {
			t.Fatal(err)
		}
		if err = node.AddRawLink("foo", &ipld.Link{
			Size: 10,
			Cid:  someCid,
		}); err != nil {
			t.Fatal(err)
		}
		if err = node.RemoveNodeLink("foo"); err != nil {
			t.Fatal(err)
		}
		verifySortedNode(t, node)
	})

	t.Run("update link should not mutate, returned ProtoNode should be sorted", func(t *testing.T) {
		node, err := DecodeProtobuf(byts)
		if err != nil {
			t.Fatal(err)
		}
		newNode, err := node.UpdateNodeLink("self", node)
		if err != nil {
			t.Fatal(err)
		}
		if err = newNode.RemoveNodeLink("self"); err != nil {
			t.Fatal(err)
		}
		verifySortedNode(t, newNode)
		verifyUnsortedNode(t, node)
	})

	t.Run("SetLinks() should mutate", func(t *testing.T) {
		node, err := DecodeProtobuf(byts)
		if err != nil {
			t.Fatal(err)
		}
		links := node.Links() // clone
		node.SetLinks(links)
		verifySortedNode(t, node)
	})
}

func TestProgressIndicator(t *testing.T) {
	testProgressIndicator(t, 5)
}

func TestProgressIndicatorNoChildren(t *testing.T) {
	testProgressIndicator(t, 0)
}

func testProgressIndicator(t *testing.T, depth int) {
	ds := dstest.Mock()

	top, numChildren := mkDag(ds, depth)

	v := new(ProgressTracker)
	ctx := v.DeriveContext(context.Background())

	err := FetchGraph(ctx, top, ds)
	if err != nil {
		t.Fatal(err)
	}

	if v.Value() != numChildren+1 {
		t.Errorf("wrong number of children reported in progress indicator, expected %d, got %d",
			numChildren+1, v.Value())
	}
}

func mkDag(ds ipld.DAGService, depth int) (cid.Cid, int) {
	ctx := context.Background()

	totalChildren := 0
	f := func() *ProtoNode {
		p := new(ProtoNode)
		buf := make([]byte, 16)
		rand.Read(buf)

		p.SetData(buf)
		err := ds.Add(ctx, p)
		if err != nil {
			panic(err)
		}
		return p
	}

	for i := 0; i < depth; i++ {
		thisf := f
		f = func() *ProtoNode {
			pn := mkNodeWithChildren(thisf, 10)
			err := ds.Add(ctx, pn)
			if err != nil {
				panic(err)
			}
			totalChildren += 10
			return pn
		}
	}

	nd := f()
	err := ds.Add(ctx, nd)
	if err != nil {
		panic(err)
	}

	return nd.Cid(), totalChildren
}

func mkNodeWithChildren(getChild func() *ProtoNode, width int) *ProtoNode {
	cur := new(ProtoNode)

	for i := 0; i < width; i++ {
		c := getChild()
		if err := cur.AddNodeLink(fmt.Sprint(i), c); err != nil {
			panic(err)
		}
	}

	return cur
}
