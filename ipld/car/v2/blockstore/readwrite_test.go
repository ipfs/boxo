package blockstore_test

import (
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ipld/go-car/v2/blockstore"

	"github.com/ipfs/go-cid"
	carv1 "github.com/ipld/go-car"
)

func TestBlockstore(t *testing.T) {
	f, err := os.Open("testdata/test.car")
	assert.NoError(t, err)
	defer f.Close()
	r, err := carv1.NewCarReader(f)
	assert.NoError(t, err)
	path := "testv2blockstore.car"
	ingester, err := blockstore.NewReadWrite(path, r.Header.Roots)
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
		assert.NoError(t, err)

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
