package unixfs_test

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/ipsl/helpers"
	. "github.com/ipfs/go-libipfs/ipsl/unixfs"
	"github.com/ipld/go-car/v2"
	"golang.org/x/exp/slices"
)

func getSmallTreeDatastore(t *testing.T) (blockservice.BlockGetter, []cid.Cid) {
	f, err := os.Open("testdata/small-tree.car")
	if err != nil {
		t.Fatalf("to open small-tree.car: %s", err)
	}
	defer f.Close()

	c, err := car.NewBlockReader(f)
	if err != nil {
		t.Fatalf("to read car header: %s", err)
	}

	var cids []cid.Cid

	bs := blockstore.NewBlockstore(datastore.NewMapDatastore())
BlockLoop:
	for {
		block, err := c.Next()
		switch err {
		case nil:
		case io.EOF:
			break BlockLoop
		default:
			t.Fatalf("to read car: %s", err)
		}

		err = bs.Put(context.Background(), block)
		if err != nil {
			t.Fatalf("to writing to datastore: %s", err)
		}

		cids = append(cids, block.Cid())
	}

	return blockservice.New(bs, offline.Exchange(blockstore.NewBlockstore(datastore.NewNullDatastore()))), cids
}

func TestEverything(t *testing.T) {
	bs, expectedOrder := getSmallTreeDatastore(t)
	root := expectedOrder[0]
	var result []cid.Cid
	err := helpers.SyncDFS(context.Background(), root, Everything(), bs, 10, func(b blocks.Block) error {
		c := b.Cid()
		hashedData, err := c.Prefix().Sum(b.RawData())
		if err != nil {
			t.Errorf("error hashing data in callBack: %s", err)
		} else {
			if !hashedData.Equals(c) {
				t.Errorf("got wrong bytes in callBack: cid %s; hashedBytes %s", c, hashedData)
			}
		}

		result = append(result, c)

		return nil
	})
	if err != nil {
		t.Fatalf("SyncDFS: %s", err)
	}

	if !slices.Equal(result, expectedOrder) {
		t.Errorf("bad traversal order: expected: %v; got %v", expectedOrder, result)
	}
}
