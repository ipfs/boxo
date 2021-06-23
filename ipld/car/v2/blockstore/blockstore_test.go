package blockstore_test

import (
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/ipld/go-car/v2/blockstore"

	"github.com/ipfs/go-cid"
	carv1 "github.com/ipld/go-car"
)

func TestBlockstore(t *testing.T) {
	f, err := os.Open("../carbs/testdata/test.car")
	if err != nil {
		t.Skipf("fixture not found: %q", err)
		return
	}
	defer f.Close()

	r, err := carv1.NewCarReader(f)
	if err != nil {
		t.Fatal(err)
	}
	path := "testv2blockstore.car"
	ingester, err := blockstore.New(path, r.Header.Roots)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.Remove(path)
	}()

	cids := make([]cid.Cid, 0)
	for {
		b, err := r.Next()
		if err == io.EOF {
			break
		}
		if err := ingester.Put(b); err != nil {
			t.Fatal(err)
		}
		cids = append(cids, b.Cid())

		// try reading a random one:
		candidate := cids[rand.Intn(len(cids))]
		if has, err := ingester.Has(candidate); !has || err != nil {
			t.Fatalf("expected to find %s but didn't: %s", candidate, err)
		}
	}

	for _, c := range cids {
		b, err := ingester.Get(c)
		if err != nil {
			t.Fatal(err)
		}
		if !b.Cid().Equals(c) {
			t.Fatal("wrong item returned")
		}
	}

	if err := ingester.Finalize(); err != nil {
		t.Fatal(err)
	}

	stat, err := os.Stat("testcarbon.car.idx")
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() <= 0 {
		t.Fatalf("index not written: %v", stat)
	}

	carb, err := blockstore.OpenReadOnly(path, true)
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range cids {
		b, err := carb.Get(c)
		if err != nil {
			t.Fatal(err)
		}
		if !b.Cid().Equals(c) {
			t.Fatal("wrong item returned")
		}
	}
}
