package carv1

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ipfs/boxo/ipld/merkledag"
	dstest "github.com/ipfs/boxo/ipld/merkledag/test"
	cid "github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
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
	a := merkledag.NewRawNode([]byte("aaaa"))
	b := merkledag.NewRawNode([]byte("bbbb"))
	c := merkledag.NewRawNode([]byte("cccc"))

	nd1 := &merkledag.ProtoNode{}
	nd1.AddNodeLink("cat", a)

	nd2 := &merkledag.ProtoNode{}
	nd2.AddNodeLink("first", nd1)
	nd2.AddNodeLink("dog", b)

	nd3 := &merkledag.ProtoNode{}
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
		has, err := bs.Has(context.TODO(), nd.Cid())
		if err != nil {
			t.Fatal(err)
		}

		if !has {
			t.Fatal("should have cid in blockstore")
		}
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
		},
		{
			"{version:\"1\",roots:[baeaaaa3bmjrq]}",
			"1da265726f6f747381d82a4800010000036162636776657273696f6e6131",
			"", "invalid header: ",
		},
		{
			"{version:1}",
			"0aa16776657273696f6e01",
			"empty car, no roots",
			"",
		},
		{
			"{version:1,roots:{cid:baeaaaa3bmjrq}}",
			"20a265726f6f7473a163636964d82a4800010000036162636776657273696f6e01",
			"",
			"invalid header: ",
		},
		{
			"{version:1,roots:[baeaaaa3bmjrq],blip:true}",
			"22a364626c6970f565726f6f747381d82a4800010000036162636776657273696f6e01",
			"",
			"invalid header: ",
		},
		{
			"[1,[]]",
			"03820180",
			"",
			"invalid header: ",
		},
		{
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
			if err == nil {
				t.Fatal("expected error from bad header, didn't get one")
			}
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

func TestCarHeaderMatchess(t *testing.T) {
	oneCid := merkledag.NewRawNode([]byte("fish")).Cid()
	anotherCid := merkledag.NewRawNode([]byte("lobster")).Cid()
	tests := []struct {
		name  string
		one   CarHeader
		other CarHeader
		want  bool
	}{
		{
			"SameVersionNilRootsIsMatching",
			CarHeader{nil, 1},
			CarHeader{nil, 1},
			true,
		},
		{
			"SameVersionEmptyRootsIsMatching",
			CarHeader{[]cid.Cid{}, 1},
			CarHeader{[]cid.Cid{}, 1},
			true,
		},
		{
			"SameVersionNonEmptySameRootsIsMatching",
			CarHeader{[]cid.Cid{oneCid}, 1},
			CarHeader{[]cid.Cid{oneCid}, 1},
			true,
		},
		{
			"SameVersionNonEmptySameRootsInDifferentOrderIsMatching",
			CarHeader{[]cid.Cid{oneCid, anotherCid}, 1},
			CarHeader{[]cid.Cid{anotherCid, oneCid}, 1},
			true,
		},
		{
			"SameVersionDifferentRootsIsNotMatching",
			CarHeader{[]cid.Cid{oneCid}, 1},
			CarHeader{[]cid.Cid{anotherCid}, 1},
			false,
		},
		{
			"DifferentVersionDifferentRootsIsNotMatching",
			CarHeader{[]cid.Cid{oneCid}, 0},
			CarHeader{[]cid.Cid{anotherCid}, 1},
			false,
		},
		{
			"MismatchingVersionIsNotMatching",
			CarHeader{nil, 0},
			CarHeader{nil, 1},
			false,
		},
		{
			"ZeroValueHeadersAreMatching",
			CarHeader{},
			CarHeader{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.one.Matches(tt.other)
			require.Equal(t, tt.want, got, "Matches() = %v, want %v", got, tt.want)
		})
	}
}

func TestReadingZeroLengthSectionWithoutOptionSetIsError(t *testing.T) {
	f, err := os.Open("../../testdata/sample-v1-with-zero-len-section.car")
	require.NoError(t, err)
	subject, err := NewCarReader(f)
	require.NoError(t, err)

	for {
		_, err := subject.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			require.EqualError(t, err, "invalid cid: varints malformed, could not reach the end")
			return
		}
	}
	require.Fail(t, "expected error when reading file with zero section without option set")
}

func TestReadingZeroLengthSectionWithOptionSetIsSuccess(t *testing.T) {
	f, err := os.Open("../../testdata/sample-v1-with-zero-len-section.car")
	require.NoError(t, err)
	subject, err := NewCarReaderWithZeroLengthSectionAsEOF(f)
	require.NoError(t, err)

	for {
		_, err := subject.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
}
