package unixfs_test

import (
	"bytes"
	"testing"

	. "github.com/ipfs/boxo/unixfs"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

func TestRaw(t *testing.T) {
	t.Parallel()
	data := []byte("👋🌍️")
	mh, err := mh.Sum(data, mh.BLAKE3, -1)
	if err != nil {
		t.Fatal()
	}
	c := cid.NewCidV1(cid.Raw, mh)

	validate := func(t *testing.T, f File) {
		if !bytes.Equal(data, f.Data) {
			t.Errorf("expected %v got %v", data, f.Data)
		}
		if l := len(f.Childrens); l != 0 {
			t.Errorf("expected 0 Childrens got %d", l)
		}
		tsize, ok := f.TSize()
		if !ok {
			t.Error("expected to find TSize but didn't")
		} else if l := uint64(len(data)); tsize != l {
			t.Errorf("expected tsize %d got %d", l, tsize)
		}
		if f.Cid != c {
			t.Errorf("expected cid %s got %s", c, f.Cid)
		}
	}

	t.Run("Parse", func(t *testing.T) {
		t.Parallel()
		b, err := blocks.NewBlockWithCid(data, c)
		if err != nil {
			t.Fatal(err)
		}
		a, err := Parse(b)
		if err != nil {
			t.Fatal(err)
		}
		f, ok := a.(File)
		if !ok {
			t.Fatalf("expected File got %T", a)
		}
		validate(t, f)
	})
	t.Run("ParseAppend", func(t *testing.T) {
		t.Parallel()
		var someArr [2]FileEntry
		typ, f, _, _, err := ParseAppend(someArr[:1], nil, c, data)
		if err != nil {
			t.Fatal(err)
		}
		if typ != TFile {
			t.Fatalf("expected %v got %v", TFile, typ)
		}
		validate(t, f)
		// Check someArr[1] to ensure it doesn't touch already existing entries before len.
		if &someArr[1] != &f.Childrens[:1][0] {
			t.Fatal("expected pointers to still be aliased but they are not")
		}
	})
}
