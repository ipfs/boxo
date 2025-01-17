package wantlist

import (
	"testing"

	pb "github.com/ipfs/boxo/bitswap/message/pb"
	cid "github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

var testcids []cid.Cid

func init() {
	strs := []string{
		"QmQL8LqkEgYXaDHdNYCG2mmpow7Sp8Z8Kt3QS688vyBeC7",
		"QmcBDsdjgSXU7BP4A4V8LJCXENE5xVwnhrhRGVTJr9YCVj",
		"QmQakgd2wDxc3uUF4orGdEm28zUT9Mmimp5pyPG2SFS9Gj",
	}
	for _, s := range strs {
		c, err := cid.Decode(s)
		if err != nil {
			panic(err)
		}
		testcids = append(testcids, c)
	}
}

type wli interface {
	Get(cid.Cid) (Entry, bool)
	Has(cid.Cid) bool
}

func assertHasCid(t *testing.T, w wli, c cid.Cid) {
	e, ok := w.Get(c)
	require.True(t, ok)
	require.Equal(t, c, e.Cid)
}

func TestBasicWantlist(t *testing.T) {
	wl := New()

	require.True(t, wl.Add(testcids[0], 5, pb.Message_Wantlist_Block))
	assertHasCid(t, wl, testcids[0])
	require.True(t, wl.Add(testcids[1], 4, pb.Message_Wantlist_Block))
	assertHasCid(t, wl, testcids[0])
	assertHasCid(t, wl, testcids[1])

	require.Equal(t, 2, wl.Len())

	require.False(t, wl.Add(testcids[1], 4, pb.Message_Wantlist_Block), "add should not report success on second add")
	assertHasCid(t, wl, testcids[0])
	assertHasCid(t, wl, testcids[1])

	require.Equal(t, 2, wl.Len())

	require.True(t, wl.RemoveType(testcids[0], pb.Message_Wantlist_Block))

	assertHasCid(t, wl, testcids[1])
	require.False(t, wl.Has(testcids[0]), "should not have this cid")
}

func TestAddHaveThenBlock(t *testing.T) {
	wl := New()

	wl.Add(testcids[0], 5, pb.Message_Wantlist_Have)
	wl.Add(testcids[0], 5, pb.Message_Wantlist_Block)

	e, ok := wl.Get(testcids[0])
	require.True(t, ok)
	require.Equal(t, pb.Message_Wantlist_Block, e.WantType)
}

func TestAddBlockThenHave(t *testing.T) {
	wl := New()

	wl.Add(testcids[0], 5, pb.Message_Wantlist_Block)
	wl.Add(testcids[0], 5, pb.Message_Wantlist_Have)

	e, ok := wl.Get(testcids[0])
	require.True(t, ok)
	require.Equal(t, pb.Message_Wantlist_Block, e.WantType)
}

func TestAddHaveThenRemoveBlock(t *testing.T) {
	wl := New()

	wl.Add(testcids[0], 5, pb.Message_Wantlist_Have)
	wl.RemoveType(testcids[0], pb.Message_Wantlist_Block)

	require.False(t, wl.Has(testcids[0]))
}

func TestAddBlockThenRemoveHave(t *testing.T) {
	wl := New()

	wl.Add(testcids[0], 5, pb.Message_Wantlist_Block)
	wl.RemoveType(testcids[0], pb.Message_Wantlist_Have)

	e, ok := wl.Get(testcids[0])
	require.True(t, ok)
	require.Equal(t, pb.Message_Wantlist_Block, e.WantType)
}

func TestAddHaveThenRemoveAny(t *testing.T) {
	wl := New()

	wl.Add(testcids[0], 5, pb.Message_Wantlist_Have)
	wl.Remove(testcids[0])

	require.False(t, wl.Has(testcids[0]))
}

func TestAddBlockThenRemoveAny(t *testing.T) {
	wl := New()

	wl.Add(testcids[0], 5, pb.Message_Wantlist_Block)
	wl.Remove(testcids[0])

	require.False(t, wl.Has(testcids[0]))
}

func TestSortEntries(t *testing.T) {
	wl := New()

	wl.Add(testcids[0], 3, pb.Message_Wantlist_Block)
	wl.Add(testcids[1], 5, pb.Message_Wantlist_Have)
	wl.Add(testcids[2], 4, pb.Message_Wantlist_Have)

	entries := wl.Entries()
	if !entries[0].Cid.Equals(testcids[1]) ||
		!entries[1].Cid.Equals(testcids[2]) ||
		!entries[2].Cid.Equals(testcids[0]) {
		t.Fatal("wrong order")
	}
}

// Test adding and removing interleaved with checking entries to make sure we clear the cache.
func TestCache(t *testing.T) {
	wl := New()

	wl.Add(testcids[0], 3, pb.Message_Wantlist_Block)
	require.Len(t, wl.Entries(), 1)

	wl.Add(testcids[1], 3, pb.Message_Wantlist_Block)
	require.Len(t, wl.Entries(), 2)

	wl.Remove(testcids[1])
	require.Len(t, wl.Entries(), 1)
}
