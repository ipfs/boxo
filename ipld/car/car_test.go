package car

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	cid "github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	dstest "github.com/ipfs/go-merkledag/test"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"
)

func assertAddNodes(t *testing.T, ds format.DAGService, nds ...format.Node) {
	for _, nd := range nds {
		if err := ds.Add(context.Background(), nd); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRoundtrip(t *testing.T) {
	dserv := dstest.Mock()
	a := dag.NewRawNode([]byte("aaaa"))
	b := dag.NewRawNode([]byte("bbbb"))
	c := dag.NewRawNode([]byte("cccc"))

	nd1 := &dag.ProtoNode{}
	nd1.AddNodeLink("cat", a)

	nd2 := &dag.ProtoNode{}
	nd2.AddNodeLink("first", nd1)
	nd2.AddNodeLink("dog", b)

	nd3 := &dag.ProtoNode{}
	nd3.AddNodeLink("second", nd2)
	nd3.AddNodeLink("bear", c)

	assertAddNodes(t, dserv, a, b, c, nd1, nd2, nd3)

	buf := new(bytes.Buffer)
	if err := WriteCar(context.Background(), dserv, []cid.Cid{nd3.Cid()}, buf); err != nil {
		t.Fatal(err)
	}

	bserv := dstest.Bserv()
	ch, err := LoadCar(bserv.Blockstore(), buf)
	if err != nil {
		t.Fatal(err)
	}

	if len(ch.Roots) != 1 {
		t.Fatal("should have one root")
	}

	if !ch.Roots[0].Equals(nd3.Cid()) {
		t.Fatal("got wrong cid")
	}

	bs := bserv.Blockstore()
	for _, nd := range []format.Node{a, b, c, nd1, nd2, nd3} {
		has, err := bs.Has(nd.Cid())
		if err != nil {
			t.Fatal(err)
		}

		if !has {
			t.Fatal("should have cid in blockstore")
		}
	}
}

func TestRoundtripSelective(t *testing.T) {
	sourceBserv := dstest.Bserv()
	sourceBs := sourceBserv.Blockstore()
	dserv := dag.NewDAGService(sourceBserv)
	a := dag.NewRawNode([]byte("aaaa"))
	b := dag.NewRawNode([]byte("bbbb"))
	c := dag.NewRawNode([]byte("cccc"))

	nd1 := &dag.ProtoNode{}
	nd1.AddNodeLink("cat", a)

	nd2 := &dag.ProtoNode{}
	nd2.AddNodeLink("first", nd1)
	nd2.AddNodeLink("dog", b)
	nd2.AddNodeLink("repeat", nd1)

	nd3 := &dag.ProtoNode{}
	nd3.AddNodeLink("second", nd2)
	nd3.AddNodeLink("bear", c)

	assertAddNodes(t, dserv, a, b, c, nd1, nd2, nd3)

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	// the graph assembled above looks as follows, in order:
	// nd3 -> [c, nd2 -> [nd1 -> a, b, nd1 -> a]]
	// this selector starts at n3, and traverses a link at index 1 (nd2, the second link, zero indexed)
	// it then recursively traverses all of its children
	// the only node skipped is 'c' -- link at index 0 immediately below nd3
	// the purpose is simply to show we are not writing the entire dag underneath
	// nd3
	selector := ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
		efsb.Insert("Links",
			ssb.ExploreIndex(1, ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))))
	}).Node()

	sc := NewSelectiveCar(context.Background(), sourceBs, []Dag{{Root: nd3.Cid(), Selector: selector}})

	// write car in one step
	buf := new(bytes.Buffer)
	blockCount := 0
	var oneStepBlocks []Block
	err := sc.Write(buf, func(block Block) error {
		oneStepBlocks = append(oneStepBlocks, block)
		blockCount++
		return nil
	})
	require.Equal(t, blockCount, 5)
	require.NoError(t, err)

	// create a new builder for two-step write
	sc2 := NewSelectiveCar(context.Background(), sourceBs, []Dag{{Root: nd3.Cid(), Selector: selector}})

	// write car in two steps
	var twoStepBlocks []Block
	scp, err := sc2.Prepare(func(block Block) error {
		twoStepBlocks = append(twoStepBlocks, block)
		return nil
	})
	require.NoError(t, err)
	buf2 := new(bytes.Buffer)
	err = scp.Dump(buf2)
	require.NoError(t, err)

	// verify preparation step correctly assesed length and blocks
	require.Equal(t, scp.Size(), uint64(buf.Len()))
	require.Equal(t, len(scp.Cids()), blockCount)

	// verify equal data written by both methods
	require.Equal(t, buf.Bytes(), buf2.Bytes())

	// verify equal blocks were passed to user block hook funcs
	require.Equal(t, oneStepBlocks, twoStepBlocks)

	// readout car and verify contents
	bserv := dstest.Bserv()
	ch, err := LoadCar(bserv.Blockstore(), buf)
	require.NoError(t, err)
	require.Equal(t, len(ch.Roots), 1)

	require.True(t, ch.Roots[0].Equals(nd3.Cid()))

	bs := bserv.Blockstore()
	for _, nd := range []format.Node{a, b, nd1, nd2, nd3} {
		has, err := bs.Has(nd.Cid())
		require.NoError(t, err)
		require.True(t, has)
	}

	for _, nd := range []format.Node{c} {
		has, err := bs.Has(nd.Cid())
		require.NoError(t, err)
		require.False(t, has)
	}
}

func TestEOFHandling(t *testing.T) {
	// fixture is a clean single-block, single-root CAR
	fixture, err := hex.DecodeString("3aa265726f6f747381d82a58250001711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80b6776657273696f6e012c01711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80ba165646f646779f5")
	if err != nil {
		t.Fatal(err)
	}

	load := func(t *testing.T, byts []byte) *CarReader {
		cr, err := NewCarReader(bytes.NewReader(byts))
		if err != nil {
			t.Fatal(err)
		}

		blk, err := cr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if blk.Cid().String() != "bafyreiavd7u6opdcm6tqmddpnrgmvfb4enxuwglhenejmchnwqvixd5ibm" {
			t.Fatal("unexpected CID")
		}

		return cr
	}

	t.Run("CleanEOF", func(t *testing.T) {
		cr := load(t, fixture)

		blk, err := cr.Next()
		if err != io.EOF {
			t.Fatal("Didn't get expected EOF")
		}
		if blk != nil {
			t.Fatal("EOF returned expected block")
		}
	})

	t.Run("BadVarint", func(t *testing.T) {
		fixtureBadVarint := append(fixture, 160)
		cr := load(t, fixtureBadVarint)

		blk, err := cr.Next()
		if err != io.ErrUnexpectedEOF {
			t.Fatal("Didn't get unexpected EOF")
		}
		if blk != nil {
			t.Fatal("EOF returned unexpected block")
		}
	})

	t.Run("TruncatedBlock", func(t *testing.T) {
		fixtureTruncatedBlock := append(fixture, 100, 0, 0)
		cr := load(t, fixtureTruncatedBlock)

		blk, err := cr.Next()
		if err != io.ErrUnexpectedEOF {
			t.Fatal("Didn't get unexpected EOF")
		}
		if blk != nil {
			t.Fatal("EOF returned unexpected block")
		}
	})
}

func TestBadHeaders(t *testing.T) {
	testCases := []struct {
		name   string
		hex    string
		errStr string // either the whole error string
		errPfx string // or just the prefix
	}{
		{
			"{version:2}",
			"0aa16776657273696f6e02",
			"invalid car version: 2",
			"",
		},
		{
			// an unfortunate error because we don't use a pointer
			"{roots:[baeaaaa3bmjrq]}",
			"13a165726f6f747381d82a480001000003616263",
			"invalid car version: 0",
			"",
		}, {
			"{version:\"1\",roots:[baeaaaa3bmjrq]}",
			"1da265726f6f747381d82a4800010000036162636776657273696f6e6131",
			"", "invalid header: ",
		}, {
			"{version:1}",
			"0aa16776657273696f6e01",
			"empty car, no roots",
			"",
		}, {
			"{version:1,roots:{cid:baeaaaa3bmjrq}}",
			"20a265726f6f7473a163636964d82a4800010000036162636776657273696f6e01",
			"",
			"invalid header: ",
		}, {
			"{version:1,roots:[baeaaaa3bmjrq],blip:true}",
			"22a364626c6970f565726f6f747381d82a4800010000036162636776657273696f6e01",
			"",
			"invalid header: ",
		}, {
			"[1,[]]",
			"03820180",
			"",
			"invalid header: ",
		}, {
			// this is an unfortunate error, it'd be nice to catch it better but it's
			// very unlikely we'd ever see this in practice
			"null",
			"01f6",
			"",
			"invalid car version: 0",
		},
	}

	makeCar := func(t *testing.T, byts string) error {
		fixture, err := hex.DecodeString(byts)
		if err != nil {
			t.Fatal(err)
		}
		_, err = NewCarReader(bytes.NewReader(fixture))
		return err
	}

	t.Run("Sanity check {version:1,roots:[baeaaaa3bmjrq]}", func(t *testing.T) {
		err := makeCar(t, "1ca265726f6f747381d82a4800010000036162636776657273696f6e01")
		if err != nil {
			t.Fatal(err)
		}
	})

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := makeCar(t, tc.hex)
			if tc.errStr != "" {
				if err.Error() != tc.errStr {
					t.Fatalf("bad error: %v", err)
				}
			} else {
				if !strings.HasPrefix(err.Error(), tc.errPfx) {
					t.Fatalf("bad error: %v", err)
				}
			}
		})
	}
}
