package blockstore

import (
	"context"
	"testing"

	blk "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	mh "github.com/multiformats/go-multihash"
)

func createTestStores() (Blockstore, *callbackDatastore) {
	cd := &callbackDatastore{f: func() {}, ds: ds.NewMapDatastore()}
	ids := NewIdStore(NewBlockstore(cd))
	return ids, cd
}

func TestIdStore(t *testing.T) {
	idhash1, _ := cid.NewPrefixV1(cid.Raw, mh.IDENTITY).Sum([]byte("idhash1"))
	idblock1, _ := blk.NewBlockWithCid([]byte("idhash1"), idhash1)
	hash1, _ := cid.NewPrefixV1(cid.Raw, mh.SHA2_256).Sum([]byte("hash1"))
	block1, _ := blk.NewBlockWithCid([]byte("hash1"), hash1)
	emptyHash, _ := cid.NewPrefixV1(cid.Raw, mh.SHA2_256).Sum([]byte("emptyHash"))
	emptyBlock, _ := blk.NewBlockWithCid([]byte{}, emptyHash)

	ids, cb := createTestStores()

	have, _ := ids.Has(bg, idhash1)
	if !have {
		t.Fatal("Has() failed on idhash")
	}

	_, err := ids.Get(bg, idhash1)
	if err != nil {
		t.Fatalf("Get() failed on idhash: %v", err)
	}

	noop := func() {}
	failIfPassThough := func() {
		t.Fatal("operation on identity hash passed though to datastore")
	}

	cb.f = failIfPassThough
	err = ids.Put(bg, idblock1)
	if err != nil {
		t.Fatal(err)
	}

	cb.f = noop
	err = ids.Put(bg, block1)
	if err != nil {
		t.Fatalf("Put() failed on normal block: %v", err)
	}

	have, _ = ids.Has(bg, hash1)
	if !have {
		t.Fatal("normal block not added to datastore")
	}

	blockSize, _ := ids.GetSize(bg, hash1)
	if blockSize == -1 {
		t.Fatal("normal block not added to datastore")
	}

	_, err = ids.Get(bg, hash1)
	if err != nil {
		t.Fatal(err)
	}

	err = ids.Put(bg, emptyBlock)
	if err != nil {
		t.Fatalf("Put() failed on normal block: %v", err)
	}

	have, _ = ids.Has(bg, emptyHash)
	if !have {
		t.Fatal("normal block not added to datastore")
	}

	blockSize, _ = ids.GetSize(bg, emptyHash)
	if blockSize != 0 {
		t.Fatal("normal block not added to datastore")
	}

	cb.f = failIfPassThough
	err = ids.DeleteBlock(bg, idhash1)
	if err != nil {
		t.Fatal(err)
	}

	cb.f = noop
	err = ids.DeleteBlock(bg, hash1)
	if err != nil {
		t.Fatal(err)
	}

	have, _ = ids.Has(bg, hash1)
	if have {
		t.Fatal("normal block not deleted from datastore")
	}

	blockSize, _ = ids.GetSize(bg, hash1)
	if blockSize > -1 {
		t.Fatal("normal block not deleted from datastore")
	}

	err = ids.DeleteBlock(bg, emptyHash)
	if err != nil {
		t.Fatal(err)
	}

	idhash2, _ := cid.NewPrefixV1(cid.Raw, mh.IDENTITY).Sum([]byte("idhash2"))
	idblock2, _ := blk.NewBlockWithCid([]byte("idhash2"), idhash2)
	hash2, _ := cid.NewPrefixV1(cid.Raw, mh.SHA2_256).Sum([]byte("hash2"))
	block2, _ := blk.NewBlockWithCid([]byte("hash2"), hash2)

	cb.f = failIfPassThough
	err = ids.PutMany(bg, []blk.Block{idblock1, idblock2})
	if err != nil {
		t.Fatal(err)
	}

	opCount := 0
	cb.f = func() {
		opCount++
	}

	err = ids.PutMany(bg, []blk.Block{block1, block2})
	if err != nil {
		t.Fatal(err)
	}
	if opCount != 4 {
		// one call to Has and Put for each Cid
		t.Fatalf("expected exactly 4 operations got %d", opCount)
	}

	opCount = 0
	err = ids.PutMany(bg, []blk.Block{idblock1, block1})
	if err != nil {
		t.Fatal(err)
	}
	if opCount != 1 {
		// just one call to Put from the normal (non-id) block
		t.Fatalf("expected exactly 1 operations got %d", opCount)
	}

	ch, err := ids.AllKeysChan(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	cnt := 0
	for c := range ch {
		cnt++
		if c.Prefix().MhType == mh.IDENTITY {
			t.Fatalf("block with identity hash found in blockstore")
		}
	}
	if cnt != 2 {
		t.Fatalf("expected exactly two keys returned by AllKeysChan got %d", cnt)
	}
}
