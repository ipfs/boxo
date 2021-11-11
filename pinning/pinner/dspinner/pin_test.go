package dspinner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"testing"
	"time"

	bs "github.com/ipfs/go-blockservice"
	mdag "github.com/ipfs/go-merkledag"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	lds "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipfspin "github.com/ipfs/go-ipfs-pinner"
	util "github.com/ipfs/go-ipfs-util"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
)

var rand = util.NewTimeSeededRand()

type fakeLogger struct {
	logging.StandardLogger
	lastError error
}

func (f *fakeLogger) Error(args ...interface{}) {
	f.lastError = errors.New(fmt.Sprint(args...))
}

func (f *fakeLogger) Errorf(format string, args ...interface{}) {
	f.lastError = fmt.Errorf(format, args...)
}

func randNode() (*mdag.ProtoNode, cid.Cid) {
	nd := new(mdag.ProtoNode)
	nd.SetData(make([]byte, 32))
	_, err := io.ReadFull(rand, nd.Data())
	if err != nil {
		panic(err)
	}
	k := nd.Cid()
	return nd, k
}

func assertPinned(t *testing.T, p ipfspin.Pinner, c cid.Cid, failmsg string) {
	_, pinned, err := p.IsPinned(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}

	if !pinned {
		t.Fatal(failmsg)
	}
}

func assertPinnedWithType(t *testing.T, p ipfspin.Pinner, c cid.Cid, mode ipfspin.Mode, failmsg string) {
	modeText, pinned, err := p.IsPinnedWithType(context.Background(), c, mode)
	if err != nil {
		t.Fatal(err)
	}

	expect, ok := ipfspin.ModeToString(mode)
	if !ok {
		t.Fatal("unrecognized pin mode")
	}

	if !pinned {
		t.Fatal(failmsg)
	}

	if mode == ipfspin.Any {
		return
	}

	if expect != modeText {
		t.Fatal("expected", expect, "pin, got", modeText)
	}
}

func assertUnpinned(t *testing.T, p ipfspin.Pinner, c cid.Cid, failmsg string) {
	_, pinned, err := p.IsPinned(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}

	if pinned {
		t.Fatal(failmsg)
	}
}

func TestPinnerBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)

	p, err := New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}

	a, ak := randNode()
	err = dserv.Add(ctx, a)
	if err != nil {
		t.Fatal(err)
	}

	// Pin A{}
	err = p.Pin(ctx, a, false)
	if err != nil {
		t.Fatal(err)
	}

	assertPinned(t, p, ak, "Failed to find key")
	assertPinnedWithType(t, p, ak, ipfspin.Direct, "Expected direct pin")

	// create new node c, to be indirectly pinned through b
	c, _ := randNode()
	err = dserv.Add(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	ck := c.Cid()

	// Create new node b, to be parent to a and c
	b, _ := randNode()
	err = b.AddNodeLink("child", a)
	if err != nil {
		t.Fatal(err)
	}
	err = b.AddNodeLink("otherchild", c)
	if err != nil {
		t.Fatal(err)
	}

	err = dserv.Add(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	bk := b.Cid()

	// recursively pin B{A,C}
	err = p.Pin(ctx, b, true)
	if err != nil {
		t.Fatal(err)
	}

	assertPinned(t, p, ck, "child of recursively pinned node not found")

	assertPinned(t, p, bk, "Pinned node not found")
	assertPinnedWithType(t, p, bk, ipfspin.Recursive, "Recursively pinned node not found")

	d, _ := randNode()
	err = d.AddNodeLink("a", a)
	if err != nil {
		panic(err)
	}
	err = d.AddNodeLink("c", c)
	if err != nil {
		panic(err)
	}

	e, _ := randNode()
	err = d.AddNodeLink("e", e)
	if err != nil {
		panic(err)
	}

	// Must be in dagserv for unpin to work
	err = dserv.Add(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	err = dserv.Add(ctx, d)
	if err != nil {
		t.Fatal(err)
	}

	// Add D{A,C,E}
	err = p.Pin(ctx, d, true)
	if err != nil {
		t.Fatal(err)
	}

	dk := d.Cid()
	assertPinned(t, p, dk, "pinned node not found.")

	cids, err := p.RecursiveKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cids) != 2 {
		t.Error("expected 2 recursive pins")
	}
	if !(bk == cids[0] || bk == cids[1]) {
		t.Error("expected recursive pin of B")
	}
	if !(dk == cids[0] || dk == cids[1]) {
		t.Error("expected recursive pin of D")
	}

	pinned, err := p.CheckIfPinned(ctx, ak, bk, ck, dk)
	if err != nil {
		t.Fatal(err)
	}
	if len(pinned) != 4 {
		t.Error("incorrect number of results")
	}
	for _, pn := range pinned {
		switch pn.Key {
		case ak:
			if pn.Mode != ipfspin.Direct {
				t.Error("A pinned with wrong mode")
			}
		case bk:
			if pn.Mode != ipfspin.Recursive {
				t.Error("B pinned with wrong mode")
			}
		case ck:
			if pn.Mode != ipfspin.Indirect {
				t.Error("C should be pinned indirectly")
			}
			if pn.Via != dk && pn.Via != bk {
				t.Error("C should be pinned via D or B")
			}
		case dk:
			if pn.Mode != ipfspin.Recursive {
				t.Error("D pinned with wrong mode")
			}
		}
	}

	cids, err = p.DirectKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cids) != 1 {
		t.Error("expected 1 direct pin")
	}
	if cids[0] != ak {
		t.Error("wrong direct pin")
	}

	cids, _ = p.InternalPins(ctx)
	if len(cids) != 0 {
		t.Error("shound not have internal keys")
	}

	err = p.Unpin(ctx, dk, false)
	if err == nil {
		t.Fatal("expected error unpinning recursive pin without specifying recursive")
	}

	// Test recursive unpin
	err = p.Unpin(ctx, dk, true)
	if err != nil {
		t.Fatal(err)
	}

	err = p.Unpin(ctx, dk, true)
	if err != ipfspin.ErrNotPinned {
		t.Fatal("expected error:", ipfspin.ErrNotPinned)
	}

	err = p.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	p, err = New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}

	// Test directly pinned
	assertPinned(t, p, ak, "Could not find pinned node!")

	// Test recursively pinned
	assertPinned(t, p, bk, "could not find recursively pinned node")

	// Remove the pin but not the index to simulate corruption
	ids, err := p.cidDIndex.Search(ctx, ak.KeyString())
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) == 0 {
		t.Fatal("did not find pin for cid", ak.String())
	}
	pp, err := p.loadPin(ctx, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if pp.Mode != ipfspin.Direct {
		t.Error("loaded pin has wrong mode")
	}
	if pp.Cid != ak {
		t.Error("loaded pin has wrong cid")
	}
	err = p.dstore.Delete(ctx, pp.dsKey())
	if err != nil {
		t.Fatal(err)
	}

	realLog := log
	fakeLog := &fakeLogger{}
	fakeLog.StandardLogger = log
	log = fakeLog
	err = p.Pin(ctx, a, true)
	if err != nil {
		t.Fatal(err)
	}
	if fakeLog.lastError == nil {
		t.Error("expected error to be logged")
	} else if fakeLog.lastError.Error() != "found CID index with missing pin" {
		t.Error("did not get expected log message")
	}

	log = realLog
}

func TestAddLoadPin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)

	p, err := New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}

	a, ak := randNode()
	err = dserv.Add(ctx, a)
	if err != nil {
		panic(err)
	}

	mode := ipfspin.Recursive
	name := "my-pin"
	pid, err := p.addPin(ctx, ak, mode, name)
	if err != nil {
		t.Fatal(err)
	}

	// Load pin and check that data decoded correctly
	pinData, err := p.loadPin(ctx, pid)
	if err != nil {
		t.Fatal(err)
	}
	if pinData.Mode != mode {
		t.Error("worng pin mode")
	}
	if pinData.Cid != ak {
		t.Error("wrong pin cid")
	}
	if pinData.Name != name {
		t.Error("wrong pin name; expected", name, "got", pinData.Name)
	}
}

func TestRemovePinWithMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)

	p, err := New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}

	a, ak := randNode()
	err = dserv.Add(ctx, a)
	if err != nil {
		panic(err)
	}

	err = p.Pin(ctx, a, false)
	if err != nil {
		t.Fatal(err)
	}

	ok, err := p.removePinsForCid(ctx, ak, ipfspin.Recursive)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("pin should not have been removed")
	}

	p.RemovePinWithMode(ak, ipfspin.Direct)

	assertUnpinned(t, p, ak, "pin was not removed")
}

func TestIsPinnedLookup(t *testing.T) {
	// Test that lookups work in pins which share
	// the same branches.  For that construct this tree:
	//
	// A5->A4->A3->A2->A1->A0
	//         /           /
	// B-------           /
	//  \                /
	//   C---------------
	//
	// This ensures that IsPinned works for all objects both when they
	// are pinned and once they have been unpinned.
	aBranchLen := 6

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)

	// Create new pinner.  New will not load anything since there are
	// no pins saved in the datastore yet.
	p, err := New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}

	aKeys, bk, ck, err := makeTree(ctx, aBranchLen, dserv, p)
	if err != nil {
		t.Fatal(err)
	}

	assertPinned(t, p, aKeys[0], "A0 should be pinned")
	assertPinned(t, p, aKeys[1], "A1 should be pinned")
	assertPinned(t, p, ck, "C should be pinned")
	assertPinned(t, p, bk, "B should be pinned")

	// Unpin A5 recursively
	if err = p.Unpin(ctx, aKeys[5], true); err != nil {
		t.Fatal(err)
	}

	assertPinned(t, p, aKeys[0], "A0 should still be pinned through B")
	assertUnpinned(t, p, aKeys[4], "A4 should be unpinned")

	// Unpin B recursively
	if err = p.Unpin(ctx, bk, true); err != nil {
		t.Fatal(err)
	}
	assertUnpinned(t, p, bk, "B should be unpinned")
	assertUnpinned(t, p, aKeys[1], "A1 should be unpinned")
	assertPinned(t, p, aKeys[0], "A0 should still be pinned through C")
}

func TestDuplicateSemantics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)

	p, err := New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}

	a, _ := randNode()
	err = dserv.Add(ctx, a)
	if err != nil {
		t.Fatal(err)
	}

	// pin is recursively
	err = p.Pin(ctx, a, true)
	if err != nil {
		t.Fatal(err)
	}

	// pinning directly should fail
	err = p.Pin(ctx, a, false)
	if err == nil {
		t.Fatal("expected direct pin to fail")
	}

	// pinning recursively again should succeed
	err = p.Pin(ctx, a, true)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)
	p, err := New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}
	_, k := randNode()

	p.PinWithMode(k, ipfspin.Recursive)
	if err = p.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	assertPinned(t, p, k, "expected key to still be pinned")
}

func TestPinRecursiveFail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)

	p, err := New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}

	a, _ := randNode()
	b, _ := randNode()
	err = a.AddNodeLink("child", b)
	if err != nil {
		t.Fatal(err)
	}

	// NOTE: This isnt a time based test, we expect the pin to fail
	mctx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()

	err = p.Pin(mctx, a, true)
	if err == nil {
		t.Fatal("should have failed to pin here")
	}

	err = dserv.Add(ctx, b)
	if err != nil {
		t.Fatal(err)
	}

	err = dserv.Add(ctx, a)
	if err != nil {
		t.Fatal(err)
	}

	// this one is time based... but shouldnt cause any issues
	mctx, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()
	err = p.Pin(mctx, a, true)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPinUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))

	dserv := mdag.NewDAGService(bserv)
	p, err := New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}
	n1, c1 := randNode()
	n2, c2 := randNode()
	_, c3 := randNode()

	if err = dserv.Add(ctx, n1); err != nil {
		t.Fatal(err)
	}
	if err = dserv.Add(ctx, n2); err != nil {
		t.Fatal(err)
	}

	if err = p.Pin(ctx, n1, true); err != nil {
		t.Fatal(err)
	}

	if err = p.Update(ctx, c1, c2, true); err != nil {
		t.Fatal(err)
	}

	assertPinned(t, p, c2, "c2 should be pinned now")
	assertUnpinned(t, p, c1, "c1 should no longer be pinned")

	if err = p.Update(ctx, c2, c1, false); err != nil {
		t.Fatal(err)
	}

	// Test updating same pin that is already pinned.
	if err = p.Update(ctx, c2, c2, true); err != nil {
		t.Fatal(err)
	}
	// Check that pin is still pinned.
	_, ok, err := p.IsPinned(ctx, c2)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("c2 should still be pinned")
	}

	// Test updating same pin that is not pinned.
	if err = p.Update(ctx, c3, c3, false); err == nil {
		t.Fatal("expected error updating unpinned cid")
	}
	_, ok, err = p.IsPinned(ctx, c3)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("c3 should not be pinned")
	}

	assertPinned(t, p, c2, "c2 should be pinned still")
	assertPinned(t, p, c1, "c1 should be pinned now")
}

func TestLoadDirty(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)

	p, err := New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}
	prev := p.SetAutosync(false)
	if !prev {
		t.Fatal("expected previous autosync to be true")
	}
	prev = p.SetAutosync(false)
	if prev {
		t.Fatal("expected previous autosync to be false")
	}
	prev = p.SetAutosync(true)
	if prev {
		t.Fatal("expected previous autosync to be false")
	}

	a, ak := randNode()
	err = dserv.Add(ctx, a)
	if err != nil {
		t.Fatal(err)
	}

	_, bk := randNode()

	err = p.Pin(ctx, a, true)
	if err != nil {
		t.Fatal(err)
	}

	cidAKey := ak.KeyString()
	cidBKey := bk.KeyString()

	// Corrupt index
	cidRIndex := p.cidRIndex
	_, err = cidRIndex.DeleteKey(ctx, cidAKey)
	if err != nil {
		t.Fatal(err)
	}
	err = cidRIndex.Add(ctx, cidBKey, "not-a-pin-id")
	if err != nil {
		t.Fatal(err)
	}

	// Force dirty, since Pin syncs automatically
	p.setDirty(ctx)

	// Verify dirty
	data, err := dstore.Get(ctx, dirtyKey)
	if err != nil {
		t.Fatalf("could not read dirty flag: %v", err)
	}
	if data[0] != 1 {
		t.Fatal("dirty flag not set")
	}

	has, err := cidRIndex.HasAny(ctx, cidAKey)
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Fatal("index should be deleted")
	}

	// Create new pinner on same datastore that was never flushed.  This should
	// detect the dirty flag and repair the indexes.
	p, err = New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}

	// Verify not dirty
	data, err = dstore.Get(ctx, dirtyKey)
	if err != nil {
		t.Fatalf("could not read dirty flag: %v", err)
	}
	if data[0] != 0 {
		t.Fatal("dirty flag is set")
	}

	// Verify index rebuilt
	cidRIndex = p.cidRIndex
	has, err = cidRIndex.HasAny(ctx, cidAKey)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("index should have been rebuilt")
	}

	has, err = p.removePinsForCid(ctx, bk, ipfspin.Any)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("expected Unpin to return true since index removed")
	}
}

func TestEncodeDecodePin(t *testing.T) {
	_, c := randNode()

	pin := newPin(c, ipfspin.Recursive, "testpin")
	pin.Metadata = make(map[string]interface{}, 2)
	pin.Metadata["hello"] = "world"
	pin.Metadata["foo"] = "bar"

	encBytes, err := encodePin(pin)
	if err != nil {
		t.Fatal(err)
	}

	decPin, err := decodePin(pin.Id, encBytes)
	if err != nil {
		t.Fatal(err)
	}

	if decPin.Id != pin.Id {
		t.Errorf("wrong pin id: expect %q got %q", pin.Id, decPin.Id)
	}
	if decPin.Cid != pin.Cid {
		t.Errorf("wrong pin cid: expect %q got %q", pin.Cid.String(), decPin.Cid.String())
	}
	if decPin.Mode != pin.Mode {
		expect, _ := ipfspin.ModeToString(pin.Mode)
		got, _ := ipfspin.ModeToString(decPin.Mode)
		t.Errorf("wrong pin mode: expect %s got %s", expect, got)
	}
	if decPin.Name != pin.Name {
		t.Errorf("wrong pin name: expect %q got %q", pin.Name, decPin.Name)
	}
	for key, val := range pin.Metadata {
		dval, ok := decPin.Metadata[key]
		if !ok {
			t.Errorf("decoded pin missing metadata key %q", key)
		}
		if dval != val {
			t.Errorf("wrong metadata value: expected %q got %q", val, dval)
		}
	}
}

func makeTree(ctx context.Context, aBranchLen int, dserv ipld.DAGService, p ipfspin.Pinner) (aKeys []cid.Cid, bk cid.Cid, ck cid.Cid, err error) {
	if aBranchLen < 3 {
		err = errors.New("set aBranchLen to at least 3")
		return
	}

	aNodes := make([]*mdag.ProtoNode, aBranchLen)
	aKeys = make([]cid.Cid, aBranchLen)
	for i := 0; i < aBranchLen; i++ {
		a, _ := randNode()
		if i >= 1 {
			if err = a.AddNodeLink("child", aNodes[i-1]); err != nil {
				return
			}
		}

		if err = dserv.Add(ctx, a); err != nil {
			return
		}
		aNodes[i] = a
		aKeys[i] = a.Cid()
	}

	// Pin last A recursively
	if err = p.Pin(ctx, aNodes[aBranchLen-1], true); err != nil {
		return
	}

	// Create node B and add A3 as child
	b, _ := randNode()
	if err = b.AddNodeLink("mychild", aNodes[3]); err != nil {
		return
	}

	// Create C node
	c, _ := randNode()
	// Add A0 as child of C
	if err = c.AddNodeLink("child", aNodes[0]); err != nil {
		return
	}

	// Add C
	if err = dserv.Add(ctx, c); err != nil {
		return
	}
	ck = c.Cid()

	// Add C to B and Add B
	if err = b.AddNodeLink("myotherchild", c); err != nil {
		return
	}
	if err = dserv.Add(ctx, b); err != nil {
		return
	}
	bk = b.Cid()

	// Pin C recursively
	if err = p.Pin(ctx, c, true); err != nil {
		return
	}

	// Pin B recursively
	if err = p.Pin(ctx, b, true); err != nil {
		return
	}

	if err = p.Flush(ctx); err != nil {
		return
	}

	return
}

func makeNodes(count int, dserv ipld.DAGService) []ipld.Node {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nodes := make([]ipld.Node, count)
	for i := 0; i < count; i++ {
		n, _ := randNode()
		err := dserv.Add(ctx, n)
		if err != nil {
			panic(err)
		}
		nodes[i] = n
	}
	return nodes
}

func pinNodes(nodes []ipld.Node, p ipfspin.Pinner, recursive bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var err error

	for i := range nodes {
		err = p.Pin(ctx, nodes[i], recursive)
		if err != nil {
			panic(err)
		}
	}
	err = p.Flush(ctx)
	if err != nil {
		panic(err)
	}
}

func unpinNodes(nodes []ipld.Node, p ipfspin.Pinner) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var err error

	for i := range nodes {
		err = p.Unpin(ctx, nodes[i].Cid(), true)
		if err != nil {
			panic(err)
		}
	}
	err = p.Flush(ctx)
	if err != nil {
		panic(err)
	}
}

type batchWrap struct {
	ds.Datastore
}

func (d *batchWrap) Batch(_ context.Context) (ds.Batch, error) {
	return ds.NewBasicBatch(d), nil
}

func makeStore() (ds.Datastore, ipld.DAGService) {
	ldstore, err := lds.NewDatastore("", nil)
	if err != nil {
		panic(err)
	}
	dstore := &batchWrap{ldstore}
	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	return dstore, dserv
}

// BenchmarkLoadRebuild loads a pinner that has some number of saved pins, and
// compares the load time when rebuilding indexes to loading without rebuilding
// indexes.
func BenchmarkLoad(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore, dserv := makeStore()
	pinner, err := New(ctx, dstore, dserv)
	if err != nil {
		panic(err.Error())
	}

	nodes := makeNodes(4096, dserv)
	pinNodes(nodes, pinner, true)

	b.Run("RebuildTrue", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err = dstore.Put(ctx, dirtyKey, []byte{1})
			if err != nil {
				panic(err.Error())
			}

			_, err = New(ctx, dstore, dserv)
			if err != nil {
				panic(err.Error())
			}
		}
	})

	b.Run("RebuildFalse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err = dstore.Put(ctx, dirtyKey, []byte{0})
			if err != nil {
				panic(err.Error())
			}

			_, err = New(ctx, dstore, dserv)
			if err != nil {
				panic(err.Error())
			}
		}
	})
}

// BenchmarkNthPins shows the time it takes to create/save 1 pin when a number
// of other pins already exist.  Each run in the series shows performance for
// creating a pin in a larger number of existing pins.
func BenchmarkNthPin(b *testing.B) {
	dstore, dserv := makeStore()
	pinner, err := New(context.Background(), dstore, dserv)
	if err != nil {
		panic(err.Error())
	}

	for count := 1000; count <= 10000; count += 1000 {
		b.Run(fmt.Sprint("PinDS-", count), func(b *testing.B) {
			benchmarkNthPin(b, count, pinner, dserv)
		})
	}
}

func benchmarkNthPin(b *testing.B, count int, pinner ipfspin.Pinner, dserv ipld.DAGService) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nodes := makeNodes(count, dserv)
	pinNodes(nodes[:count-1], pinner, true)
	b.ResetTimer()

	which := count - 1
	for i := 0; i < b.N; i++ {
		// Pin the Nth node and Flush
		err := pinner.Pin(ctx, nodes[which], true)
		if err != nil {
			panic(err)
		}
		err = pinner.Flush(ctx)
		if err != nil {
			panic(err)
		}
		// Unpin the nodes so that it can pinned next iter.
		b.StopTimer()
		err = pinner.Unpin(ctx, nodes[which].Cid(), true)
		if err != nil {
			panic(err)
		}
		err = pinner.Flush(ctx)
		if err != nil {
			panic(err)
		}
		b.StartTimer()
	}
}

// BenchmarkNPins demonstrates creating individual pins.  Each run in the
// series shows performance for a larger number of individual pins.
func BenchmarkNPins(b *testing.B) {
	for count := 128; count < 16386; count <<= 1 {
		b.Run(fmt.Sprint("PinDS-", count), func(b *testing.B) {
			dstore, dserv := makeStore()
			pinner, err := New(context.Background(), dstore, dserv)
			if err != nil {
				panic(err.Error())
			}
			benchmarkNPins(b, count, pinner, dserv)
		})
	}
}

func benchmarkNPins(b *testing.B, count int, pinner ipfspin.Pinner, dserv ipld.DAGService) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nodes := makeNodes(count, dserv)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Pin all the nodes one at a time.
		for j := range nodes {
			err := pinner.Pin(ctx, nodes[j], true)
			if err != nil {
				panic(err)
			}
			err = pinner.Flush(ctx)
			if err != nil {
				panic(err)
			}
		}

		// Unpin all nodes so that they can be pinned next iter.
		b.StopTimer()
		unpinNodes(nodes, pinner)
		b.StartTimer()
	}
}

// BenchmarkNUnpins demonstrates unpinning individual pins. Each run in the
// series shows performance for a larger number of individual unpins.
func BenchmarkNUnpins(b *testing.B) {
	for count := 128; count < 16386; count <<= 1 {
		b.Run(fmt.Sprint("UnpinDS-", count), func(b *testing.B) {
			dstore, dserv := makeStore()
			pinner, err := New(context.Background(), dstore, dserv)
			if err != nil {
				panic(err.Error())
			}
			benchmarkNUnpins(b, count, pinner, dserv)
		})
	}
}

func benchmarkNUnpins(b *testing.B, count int, pinner ipfspin.Pinner, dserv ipld.DAGService) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nodes := makeNodes(count, dserv)
	pinNodes(nodes, pinner, true)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range nodes {
			// Unpin nodes one at a time.
			err := pinner.Unpin(ctx, nodes[j].Cid(), true)
			if err != nil {
				panic(err)
			}
			err = pinner.Flush(ctx)
			if err != nil {
				panic(err)
			}
		}
		// Pin all nodes so that they can be unpinned next iter.
		b.StopTimer()
		pinNodes(nodes, pinner, true)
		b.StartTimer()
	}
}

// BenchmarkPinAllSeries shows times to pin all nodes with only one Flush at
// the end.
func BenchmarkPinAll(b *testing.B) {
	for count := 128; count < 16386; count <<= 1 {
		b.Run(fmt.Sprint("PinAllDS-", count), func(b *testing.B) {
			dstore, dserv := makeStore()
			pinner, err := New(context.Background(), dstore, dserv)
			if err != nil {
				panic(err)
			}
			benchmarkPinAll(b, count, pinner, dserv)
		})
	}
}

func benchmarkPinAll(b *testing.B, count int, pinner ipfspin.Pinner, dserv ipld.DAGService) {
	nodes := makeNodes(count, dserv)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pinNodes(nodes, pinner, true)

		b.StopTimer()
		unpinNodes(nodes, pinner)
		b.StartTimer()
	}
}

func BenchmarkRebuild(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore, dserv := makeStore()
	pinIncr := 32768

	for pins := pinIncr; pins <= pinIncr*5; pins += pinIncr {
		pinner, err := New(ctx, dstore, dserv)
		if err != nil {
			panic(err.Error())
		}
		nodes := makeNodes(pinIncr, dserv)
		pinNodes(nodes, pinner, true)

		b.Run(fmt.Sprintf("Rebuild %d", pins), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				err = dstore.Put(ctx, dirtyKey, []byte{1})
				if err != nil {
					panic(err.Error())
				}

				_, err = New(ctx, dstore, dserv)
				if err != nil {
					panic(err.Error())
				}
			}
		})
	}
}

func TestCidIndex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore, dserv := makeStore()
	pinner, err := New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}
	nodes := makeNodes(1, dserv)
	node := nodes[0]

	c := node.Cid()
	cidKey := c.KeyString()

	// Pin the cid
	pid, err := pinner.addPin(ctx, c, ipfspin.Recursive, "")
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Added pin:", pid)
	t.Log("CID index:", c.String(), "-->", pid)

	// Check that the index exists
	ok, err := pinner.cidRIndex.HasAny(ctx, cidKey)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("R-index has no value for", cidKey)
	}

	// Check that searching for the cid returns a value
	values, err := pinner.cidRIndex.Search(ctx, cidKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 {
		t.Fatal("expect index to return one value")
	}
	if values[0] != pid {
		t.Fatal("indexer should have has value", cidKey, "-->", pid)
	}

	// Check that index has specific value
	ok, err = pinner.cidRIndex.HasValue(ctx, cidKey, pid)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("indexer should have has value", cidKey, "-->", pid)
	}

	// Iterate values of index
	var seen bool
	err = pinner.cidRIndex.ForEach(ctx, "", func(key, value string) bool {
		if seen {
			t.Fatal("expected one key-value pair")
		}
		if key != cidKey {
			t.Fatal("unexpected key:", key)
		}
		if value != pid {
			t.Fatal("unexpected value:", value)
		}
		seen = true
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	// Load all pins from the datastore.
	q := query.Query{
		Prefix: pinKeyPath,
	}
	results, err := pinner.dstore.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer results.Close()

	// Iterate all pins and check if the corresponding recursive or direct
	// index is missing.  If the index is missing then create the index.
	seen = false
	for r := range results.Next() {
		if seen {
			t.Fatal("has more than one pin")
		}
		if r.Error != nil {
			t.Fatal(fmt.Errorf("cannot read index: %v", r.Error))
		}
		ent := r.Entry
		pp, err := decodePin(path.Base(ent.Key), ent.Value)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("Found pin:", pp.Id)
		if pp.Id != pid {
			t.Fatal("ID of loaded pin is not the same known to indexer")
		}
		seen = true
	}
}

func TestRebuild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore, dserv := makeStore()
	pinner, err := New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}
	nodes := makeNodes(3, dserv)
	pinNodes(nodes, pinner, true)

	c1 := nodes[0].Cid()
	cid1Key := c1.KeyString()
	c2 := nodes[1].Cid()
	cid2Key := c2.KeyString()
	c3 := nodes[2].Cid()
	cid3Key := c3.KeyString()

	// Get pin IDs
	values, err := pinner.cidRIndex.Search(ctx, cid1Key)
	if err != nil {
		t.Fatal(err)
	}
	pid1 := values[0]
	values, err = pinner.cidRIndex.Search(ctx, cid2Key)
	if err != nil {
		t.Fatal(err)
	}
	pid2 := values[0]
	values, err = pinner.cidRIndex.Search(ctx, cid3Key)
	if err != nil {
		t.Fatal(err)
	}
	pid3 := values[0]

	// Corrupt by adding direct index when there is already a recursive index
	err = pinner.cidDIndex.Add(ctx, cid1Key, pid1)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt index by deleting cid index 2 to simulate an incomplete add or delete
	_, err = pinner.cidRIndex.DeleteKey(ctx, cid2Key)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt index by deleting pin to simulate corruption
	var pp *pin
	pp, err = pinner.loadPin(ctx, pid3)
	if err != nil {
		t.Fatal(err)
	}
	err = pinner.dstore.Delete(ctx, pp.dsKey())
	if err != nil {
		t.Fatal(err)
	}

	pinner.setDirty(ctx)

	// Rebuild indexes
	pinner, err = New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}

	// Verify that indexes have same values as before
	err = verifyIndexValue(ctx, pinner, cid1Key, pid1)
	if err != nil {
		t.Fatal(err)
	}
	err = verifyIndexValue(ctx, pinner, cid2Key, pid2)
	if err != nil {
		t.Fatal(err)
	}
	err = verifyIndexValue(ctx, pinner, cid3Key, pid3)
	if err != nil {
		t.Fatal(err)
	}
}

func verifyIndexValue(ctx context.Context, pinner *pinner, cidKey, expectedPid string) error {
	values, err := pinner.cidRIndex.Search(ctx, cidKey)
	if err != nil {
		return err
	}
	if len(values) != 1 {
		return errors.New("expected 1 value")
	}
	if expectedPid != values[0] {
		return errors.New("index has wrong value")
	}
	ok, err := pinner.cidDIndex.HasAny(ctx, cidKey)
	if err != nil {
		return err
	}
	if ok {
		return errors.New("should not have a direct index")
	}
	return nil
}
