package simple_test

import (
	"context"
	"testing"
	"time"

	bsrv "github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	mock "github.com/ipfs/go-ipfs-routing/mock"
	cbor "github.com/ipfs/go-ipld-cbor"
	merkledag "github.com/ipfs/go-merkledag"
	peer "github.com/libp2p/go-libp2p-core/peer"
	testutil "github.com/libp2p/go-libp2p-testing/net"
	mh "github.com/multiformats/go-multihash"

	. "github.com/ipfs/go-ipfs-provider/simple"
)

func setupRouting(t *testing.T) (clA, clB mock.Client, idA, idB peer.ID) {
	mrserv := mock.NewServer()

	iidA := testutil.RandIdentityOrFatal(t)
	iidB := testutil.RandIdentityOrFatal(t)

	clA = mrserv.Client(iidA)
	clB = mrserv.Client(iidB)

	return clA, clB, iidA.ID(), iidB.ID()
}

func setupDag(t *testing.T) (nodes []cid.Cid, bstore blockstore.Blockstore) {
	bstore = blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	for _, data := range []string{"foo", "bar"} {
		blk, err := cbor.WrapObject(data, mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		err = bstore.Put(blk)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, blk.Cid())

		blk, err = cbor.WrapObject(map[string]interface{}{
			"child": blk.Cid(),
		}, mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		err = bstore.Put(blk)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, blk.Cid())
	}

	return nodes, bstore
}

func TestReprovide(t *testing.T) {
	testReprovide(t, func(r *Reprovider, ctx context.Context) error {
		return r.Reprovide()
	})
}

func TestTrigger(t *testing.T) {
	testReprovide(t, func(r *Reprovider, ctx context.Context) error {
		go r.Run()
		time.Sleep(1 * time.Second)
		defer r.Close()
		err := r.Trigger(ctx)
		return err
	})
}

func testReprovide(t *testing.T, trigger func(r *Reprovider, ctx context.Context) error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clA, clB, idA, _ := setupRouting(t)
	nodes, bstore := setupDag(t)

	keyProvider := NewBlockstoreProvider(bstore)
	reprov := NewReprovider(ctx, time.Hour, clA, keyProvider)
	err := trigger(reprov, ctx)
	if err != nil {
		t.Fatal(err)
	}

	var providers []peer.AddrInfo
	maxProvs := 100

	for _, c := range nodes {
		provChan := clB.FindProvidersAsync(ctx, c, maxProvs)
		for p := range provChan {
			providers = append(providers, p)
		}

		if len(providers) == 0 {
			t.Fatal("Should have gotten a provider")
		}

		if providers[0].ID != idA {
			t.Fatal("Somehow got the wrong peer back as a provider.")
		}
	}
}

func TestTriggerTwice(t *testing.T) {
	// Ensure we can only trigger once at a time.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clA, _, _, _ := setupRouting(t)

	keyCh := make(chan cid.Cid)
	startCh := make(chan struct{})
	keyFunc := func(ctx context.Context) (<-chan cid.Cid, error) {
		<-startCh
		return keyCh, nil
	}

	reprov := NewReprovider(ctx, time.Hour, clA, keyFunc)
	go reprov.Run()
	defer reprov.Close()

	// Wait for the reprovider to start, otherwise, the reprovider will
	// think a concurrent reprovide is running.
	//
	// We _could_ fix this race... but that would be complexity for nothing.
	// 1. We start a reprovide 1 minute after startup anyways.
	// 2. The window is really narrow.
	time.Sleep(1 * time.Second)

	errCh := make(chan error, 2)

	// Trigger in the background
	go func() {
		errCh <- reprov.Trigger(ctx)
	}()

	// Wait for the trigger to really start.
	startCh <- struct{}{}

	start := time.Now()
	// Try to trigger again, this should fail immediately.
	if err := reprov.Trigger(ctx); err == nil {
		t.Fatal("expected an error")
	}
	if time.Since(start) > 10*time.Millisecond {
		t.Fatal("expected reprovide to fail instantly")
	}

	// Let the trigger progress.
	close(keyCh)

	// Check the result.
	err := <-errCh
	if err != nil {
		t.Fatal(err)
	}

	// Try to trigger again, this should work.
	go func() {
		errCh <- reprov.Trigger(ctx)
	}()
	startCh <- struct{}{}
	err = <-errCh
	if err != nil {
		t.Fatal(err)
	}
}

type mockPinner struct {
	recursive []cid.Cid
	direct    []cid.Cid
}

func (mp *mockPinner) DirectKeys(ctx context.Context) ([]cid.Cid, error) {
	return mp.direct, nil
}

func (mp *mockPinner) RecursiveKeys(ctx context.Context) ([]cid.Cid, error) {
	return mp.recursive, nil
}

func TestReprovidePinned(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes, bstore := setupDag(t)

	dag := merkledag.NewDAGService(bsrv.New(bstore, offline.Exchange(bstore)))

	for i := 0; i < 2; i++ {
		clA, clB, idA, _ := setupRouting(t)

		onlyRoots := i == 0
		t.Logf("only roots: %v", onlyRoots)

		var provide, dont []cid.Cid
		if onlyRoots {
			provide = []cid.Cid{nodes[1], nodes[3]}
			dont = []cid.Cid{nodes[0], nodes[2]}
		} else {
			provide = []cid.Cid{nodes[0], nodes[1], nodes[3]}
			dont = []cid.Cid{nodes[2]}
		}

		keyProvider := NewPinnedProvider(onlyRoots, &mockPinner{
			recursive: []cid.Cid{nodes[1]},
			direct:    []cid.Cid{nodes[3]},
		}, dag)

		reprov := NewReprovider(ctx, time.Hour, clA, keyProvider)
		err := reprov.Reprovide()
		if err != nil {
			t.Fatal(err)
		}

		for i, c := range provide {
			prov, ok := <-clB.FindProvidersAsync(ctx, c, 1)
			if !ok {
				t.Errorf("Should have gotten a provider for %d", i)
				continue
			}

			if prov.ID != idA {
				t.Errorf("Somehow got the wrong peer back as a provider.")
				continue
			}
		}
		for i, c := range dont {
			prov, ok := <-clB.FindProvidersAsync(ctx, c, 1)
			if ok {
				t.Fatalf("found provider %s for %d, expected none", prov.ID, i)
			}
		}
	}
}
