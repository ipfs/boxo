package offline

import (
	"context"
	"testing"

	blockstore "github.com/ipfs/boxo/blockstore"
	u "github.com/ipfs/boxo/util"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-test/random"
)

const blockSize = 4

func TestBlockReturnsErr(t *testing.T) {
	off := Exchange(bstore())
	c := cid.NewCidV0(u.Hash([]byte("foo")))
	_, err := off.GetBlock(context.Background(), c)
	if err != nil {
		return // as desired
	}
	t.Fail()
}

func TestGetBlocks(t *testing.T) {
	store := bstore()
	ex := Exchange(store)

	expected := random.BlocksOfSize(2, blockSize)

	for _, b := range expected {
		if err := store.Put(context.Background(), b); err != nil {
			t.Fatal(err)
		}
		if err := ex.NotifyNewBlocks(context.Background(), b); err != nil {
			t.Fail()
		}
	}

	request := func() []cid.Cid {
		var ks []cid.Cid

		for _, b := range expected {
			ks = append(ks, b.Cid())
		}
		return ks
	}()

	received, err := ex.GetBlocks(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	var count int
	for range received {
		count++
	}
	if len(expected) != count {
		t.Fail()
	}
}

func bstore() blockstore.Blockstore {
	return blockstore.NewBlockstore(ds_sync.MutexWrap(ds.NewMapDatastore()))
}
