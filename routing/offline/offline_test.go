package offline

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p-kad-dht/records"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/core/test"
	mh "github.com/multiformats/go-multihash"
)

type blankValidator struct{}

func (blankValidator) Validate(_ string, _ []byte) error        { return nil }
func (blankValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

func TestOfflineRouterStorage(t *testing.T) {
	ctx := context.Background()

	nds := ds.NewMapDatastore()
	offline := NewOfflineRouter(nds, blankValidator{})

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

	local, err := offline.GetValue(ctx, "key", routing.Offline)
	if err != nil {
		t.Fatal(err)
	}

	_, err = offline.GetValue(ctx, "notHere", routing.Offline)
	if err == nil {
		t.Fatal("Router should throw errors for unfound records")
	}

	if !bytes.Equal([]byte("testing 1 2 3"), local) {
		t.Fatal("OfflineRouter does not properly store")
	}
}

// errExpiredValidator accepts every record except those whose value is
// "expired", mimicking a validity window like the IPNS EOL.
type errExpiredValidator struct{}

func (errExpiredValidator) Validate(_ string, value []byte) error {
	if bytes.Equal(value, []byte("expired")) {
		return errors.New("record is expired")
	}
	return nil
}

func (errExpiredValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

// TestOfflineRouterGetValidates ensures GetValue never returns a stored
// record that no longer passes validation, e.g. an IPNS record that
// passed its EOL after it was stored.
func TestOfflineRouterGetValidates(t *testing.T) {
	ctx := context.Background()

	nds := ds.NewMapDatastore()

	// Store a record that passes validation at write time, bypassing
	// PutValue's own validation, as happens when a record expires after
	// it was stored.
	vs := records.NewValueStore(nds, blankValidator{}, amino.DefaultMaxRecordAge)
	if err := vs.Put(ctx, "/v/key", record.MakePutRecord("/v/key", []byte("expired"))); err != nil {
		t.Fatal(err)
	}

	offline := NewOfflineRouter(nds, errExpiredValidator{})
	if _, err := offline.GetValue(ctx, "/v/key"); err == nil {
		t.Fatal("GetValue returned a record that fails validation")
	}
}

// TestOfflineRouterSharesDHTLayout pins the datastore layout contract:
// the offline router and the DHT's value store must read each other's
// records, so that a node sharing one datastore between them can
// resolve offline what was published online and vice versa.
func TestOfflineRouterSharesDHTLayout(t *testing.T) {
	ctx := context.Background()

	nds := ds.NewMapDatastore()
	offline := NewOfflineRouter(nds, blankValidator{})
	dhtVS := records.NewValueStore(nds, blankValidator{}, amino.DefaultMaxRecordAge)

	// offline write -> DHT read
	if err := offline.PutValue(ctx, "/v/offline", []byte("a")); err != nil {
		t.Fatal(err)
	}
	rec, err := dhtVS.Get(ctx, "/v/offline")
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || !bytes.Equal(rec.GetValue(), []byte("a")) {
		t.Fatal("DHT value store does not see offline-published record")
	}

	// DHT write -> offline read
	if err := dhtVS.Put(ctx, "/v/online", record.MakePutRecord("/v/online", []byte("b"))); err != nil {
		t.Fatal(err)
	}
	val, err := offline.GetValue(ctx, "/v/online")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, []byte("b")) {
		t.Fatal("offline router does not see DHT-published record")
	}
}

// TestOfflineRouterMaxRecordAge checks record retention: with no option a
// stored record is served indefinitely (retention is left to the record's own
// validity), while WithMaxRecordAge caps it by store age. Time is faked with
// synctest so the test does not wait for real hours to pass.
func TestOfflineRouterMaxRecordAge(t *testing.T) {
	t.Run("no cap by default", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := context.Background()
			r := NewOfflineRouter(ds.NewMapDatastore(), blankValidator{})

			if err := r.PutValue(ctx, "/v/key", []byte("v")); err != nil {
				t.Fatal(err)
			}
			time.Sleep(90 * 24 * time.Hour)
			val, err := r.GetValue(ctx, "/v/key")
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(val, []byte("v")) {
				t.Fatalf("got %q, want %q", val, "v")
			}
		})
	})

	t.Run("WithMaxRecordAge caps retention", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := context.Background()
			maxAge := 48 * time.Hour
			r := NewOfflineRouter(ds.NewMapDatastore(), blankValidator{}, WithMaxRecordAge(maxAge))

			if err := r.PutValue(ctx, "/v/key", []byte("v")); err != nil {
				t.Fatal(err)
			}
			val, err := r.GetValue(ctx, "/v/key")
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(val, []byte("v")) {
				t.Fatalf("got %q, want %q", val, "v")
			}

			time.Sleep(maxAge + time.Minute)
			if _, err := r.GetValue(ctx, "/v/key"); !errors.Is(err, routing.ErrNotFound) {
				t.Fatalf("want ErrNotFound after max record age, got %v", err)
			}
		})
	})
}

func TestOfflineRouterLocal(t *testing.T) {
	ctx := context.Background()

	nds := ds.NewMapDatastore()
	offline := NewOfflineRouter(nds, blankValidator{})

	id, _ := test.RandPeerID()
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
