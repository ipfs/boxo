package offline

import (
	"bytes"
	"context"
	"testing"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	testutil "github.com/libp2p/go-testutil"
	mh "github.com/multiformats/go-multihash"
)

func TestOfflineRouterStorage(t *testing.T) {
	ctx := context.Background()

	nds := ds.NewMapDatastore()
	privkey, _, _ := testutil.RandTestKeyPair(128)
	offline := NewOfflineRouter(nds, privkey)

	if err := offline.PutValue(ctx, "key", []byte("testing 1 2 3")); err != nil {
		t.Fatal(err)
	}

	val, err := offline.GetValue(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal([]byte("testing 1 2 3"), val) {
		t.Fatal("OfflineRouter does not properly store")
	}

	_, err = offline.GetValue(ctx, "notHere")
	if err == nil {
		t.Fatal("Router should throw errors for unfound records")
	}

	recVal, err := offline.GetValues(ctx, "key", 0)
	if err != nil {
		t.Fatal(err)
	}

	_, err = offline.GetValues(ctx, "notHere", 0)
	if err == nil {
		t.Fatal("Router should throw errors for unfound records")
	}

	local := recVal[0].Val
	if !bytes.Equal([]byte("testing 1 2 3"), local) {
		t.Fatal("OfflineRouter does not properly store")
	}
}

func TestOfflineRouterLocal(t *testing.T) {
	ctx := context.Background()

	nds := ds.NewMapDatastore()
	privkey, _, _ := testutil.RandTestKeyPair(128)
	offline := NewOfflineRouter(nds, privkey)

	id, _ := testutil.RandPeerID()
	_, err := offline.FindPeer(ctx, id)
	if err != ErrOffline {
		t.Fatal("OfflineRouting should alert that its offline")
	}

	h, _ := mh.Sum([]byte("test data1"), mh.SHA2_256, -1)
	c1 := cid.NewCidV0(h)
	pChan := offline.FindProvidersAsync(ctx, c1, 1)
	p, ok := <-pChan
	if ok {
		t.Fatalf("FindProvidersAsync did not return a closed channel. Instead we got %+v !", p)
	}

	h2, _ := mh.Sum([]byte("test data1"), mh.SHA2_256, -1)
	c2 := cid.NewCidV0(h2)
	err = offline.Provide(ctx, c2, true)
	if err != ErrOffline {
		t.Fatal("OfflineRouting should alert that its offline")
	}

	err = offline.Bootstrap(ctx)
	if err != nil {
		t.Fatal("You shouldn't be able to bootstrap offline routing.")
	}
}
