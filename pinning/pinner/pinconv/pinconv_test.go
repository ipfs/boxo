package pinconv

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	bs "github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	lds "github.com/ipfs/go-ds-leveldb"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipfspin "github.com/ipfs/go-ipfs-pinner"
	"github.com/ipfs/go-ipfs-pinner/dspinner"
	util "github.com/ipfs/go-ipfs-util"
	ipld "github.com/ipfs/go-ipld-format"
	mdag "github.com/ipfs/go-merkledag"
)

var rand = util.NewTimeSeededRand()

type batchWrap struct {
	ds.Datastore
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

func (d *batchWrap) Batch() (ds.Batch, error) {
	return ds.NewBasicBatch(d), nil
}

func makeStore() (ds.Datastore, ipld.DAGService) {
	ldstore, err := lds.NewDatastore("", nil)
	if err != nil {
		panic(err)
	}
	var dstore ds.Batching
	dstore = &batchWrap{ldstore}

	bstore := blockstore.NewBlockstore(dstore)
	bserv := bs.New(bstore, offline.Exchange(bstore))
	dserv := mdag.NewDAGService(bserv)
	return dstore, dserv
}

func TestConversions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dstore, dserv := makeStore()

	dsPinner, err := dspinner.New(ctx, dstore, dserv)
	if err != nil {
		t.Fatal(err)
	}

	a, ak := randNode()
	err = dsPinner.Pin(ctx, a, false)
	if err != nil {
		t.Fatal(err)
	}

	// create new node c, to be indirectly pinned through b
	c, ck := randNode()
	dserv.Add(ctx, c)

	// Create new node b, to be parent to a and c
	b, _ := randNode()
	b.AddNodeLink("child", a)
	b.AddNodeLink("otherchild", c)
	bk := b.Cid() // CID changed after adding links

	// recursively pin B{A,C}
	err = dsPinner.Pin(ctx, b, true)
	if err != nil {
		t.Fatal(err)
	}

	err = dsPinner.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}

	verifyPins := func(pinner ipfspin.Pinner) error {
		pinned, err := pinner.CheckIfPinned(ctx, ak, bk, ck)
		if err != nil {
			return err
		}
		if len(pinned) != 3 {
			return errors.New("incorrect number of results")
		}
		for _, pn := range pinned {
			switch pn.Key {
			case ak:
				if pn.Mode != ipfspin.Direct {
					return errors.New("A pinned with wrong mode")
				}
			case bk:
				if pn.Mode != ipfspin.Recursive {
					return errors.New("B pinned with wrong mode")
				}
			case ck:
				if pn.Mode != ipfspin.Indirect {
					return errors.New("C should be pinned indirectly")
				}
				if pn.Via != bk {
					return errors.New("C should be pinned via B")
				}
			}
		}
		return nil
	}

	err = verifyPins(dsPinner)
	if err != nil {
		t.Fatal(err)
	}

	ipldPinner, toIPLDCount, err := ConvertPinsFromDSToIPLD(ctx, dstore, dserv, dserv)
	if err != nil {
		t.Fatal(err)
	}
	if toIPLDCount != 2 {
		t.Fatal("expected 2 ds-to-ipld pins, got", toIPLDCount)
	}

	err = verifyPins(ipldPinner)
	if err != nil {
		t.Fatal(err)
	}

	toDSPinner, toDSCount, err := ConvertPinsFromIPLDToDS(ctx, dstore, dserv, dserv)
	if err != nil {
		t.Fatal(err)
	}
	if toDSCount != toIPLDCount {
		t.Fatal("ds-to-ipld pins", toIPLDCount, "not equal to ipld-to-ds-pins", toDSCount)
	}

	err = verifyPins(toDSPinner)
	if err != nil {
		t.Fatal(err)
	}
}

func TestConvertLoadError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore, dserv := makeStore()
	// Point /local/pins to empty node to cause failure loading pins.
	pinDatastoreKey := ds.NewKey("/local/pins")
	emptyKey, err := cid.Decode("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n")
	if err != nil {
		panic(err)
	}
	if err = dstore.Put(pinDatastoreKey, emptyKey.Bytes()); err != nil {
		panic(err)
	}
	if err = dstore.Sync(pinDatastoreKey); err != nil {
		panic(err)
	}

	_, _, err = ConvertPinsFromIPLDToDS(ctx, dstore, dserv, dserv)
	if err == nil || !strings.HasPrefix(err.Error(), "cannot load recursive keys") {
		t.Fatal("did not get expected error")
	}
}
