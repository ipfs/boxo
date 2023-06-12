package unixfs_test

import (
	"bytes"
	"encoding/base64"
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

func TestFilePB(t *testing.T) {
	t.Parallel()
	data, err := base64.StdEncoding.DecodeString(`EisKIhIgVuq+9ViNicx1O8bIsb978a8u1uoTjm4taEeNW7gcB+cSABiu1OAVEioKIhIg7XyJKU3lrLCYFLKmcNTtKc82BUBCi5ePAeAqz2M1pWYSABirmGcKEAgCGPGUxxYggIDgFSDxlGc=`)
	if err != nil {
		t.Fatal(err)
	}
	mh, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		t.Fatal()
	}
	c := cid.NewCidV0(mh)

	const firstChildrenTSize = 45623854
	const secondChildrenTSize = 1690667
	expectedChildrens := [2]FileEntry{
		FileEntryWithTSize(cid.MustParse("QmUBwP7RczPWbJSCpR4BygzvTNbJ2sfjt5yuRphSVYaJar"), 45613056, firstChildrenTSize),
		FileEntryWithTSize(cid.MustParse("QmeKhUSkRVDFbxssXpnb15UQf25YdWN9Ck3rjfZA3tvD8h"), 1690225, secondChildrenTSize),
	}

	validate := func(t *testing.T, f File) {
		if f.Cid != c {
			t.Errorf("expected %v cid got %v", c, f.Cid)
		}

		if len(f.Data) != 0 {
			t.Errorf("got unexpected data %q", f.Data)
		}

		tSize, ok := f.TSize()
		if !ok {
			t.Error("missing TSize")
		} else if et := uint64(len(data)) + firstChildrenTSize + secondChildrenTSize; tSize != et {
			t.Errorf("tSize expected %d got %d", et, tSize)
		}

		if len(f.Childrens) != 2 {
			t.Errorf("expected 2 childrens got %v", f.Childrens)
		} else if *(*[2]FileEntry)(f.Childrens) != expectedChildrens {
			t.Errorf("childrens don't match, expected %v got %v", expectedChildrens, f.Childrens)
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
		var someArr [3]FileEntry
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
