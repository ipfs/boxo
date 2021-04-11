package carbon_test

import (
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	"github.com/willscott/carbon"
	"github.com/willscott/carbs"
)

func TestCarbon(t *testing.T) {
	f, err := os.Open("test.car")
	if err != nil {
		t.Skipf("fixture not found: %q", err)
		return
	}
	defer f.Close()

	ingester, err := carbon.New("testcarbon.car")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.Remove("testcarbon.car")
	}()

	r, err := car.NewCarReader(f)
	if err != nil {
		t.Fatal(err)
	}
	cids := make([]cid.Cid, 0)
	for true {
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

	if err := ingester.Finish(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.Remove("testcarbon.car.idx")
	}()

	stat, err := os.Stat("testcarbon.car.idx")
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() <= 0 {
		t.Fatalf("index not written: %v", stat)
	}

	carb, err := carbs.Load("testcarbon.car", true)
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
