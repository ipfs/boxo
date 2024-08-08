package queue

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/boxo/internal/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-test/random"
)

const blockSize = 4

func makeCids(n int) []cid.Cid {
	blks := random.BlocksOfSize(n, blockSize)
	cids := make([]cid.Cid, n)
	for i := 0; i < n; i++ {
		cids[i] = blks[i].Cid()
	}
	return cids
}

func assertOrdered(cids []cid.Cid, q *Queue, t *testing.T) {
	for _, c := range cids {
		select {
		case dequeued := <-q.dequeue:
			if c != dequeued {
				t.Fatalf("Error in ordering of CIDs retrieved from queue. Expected: %s, got: %s", c, dequeued)
			}

		case <-time.After(time.Second * 1):
			t.Fatal("Timeout waiting for cids to be provided.")
		}
	}
}

func TestBasicOperation(t *testing.T) {
	test.Flaky(t)
	ctx := context.Background()
	defer ctx.Done()

	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := NewQueue(ds)
	defer queue.Close()

	cids := makeCids(10)

	for _, c := range cids {
		queue.Enqueue(c)
	}

	assertOrdered(cids, queue, t)
}

func TestMangledData(t *testing.T) {
	test.Flaky(t)
	ctx := context.Background()
	defer ctx.Done()

	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := NewQueue(ds)
	defer queue.Close()

	cids := makeCids(10)
	for _, c := range cids {
		queue.Enqueue(c)
	}

	// put bad data in the queue
	queueKey := datastore.NewKey("/test/0")
	err := queue.ds.Put(ctx, queueKey, []byte("borked"))
	if err != nil {
		t.Fatal(err)
	}

	// expect to only see the valid cids we entered
	expected := cids
	assertOrdered(expected, queue, t)
}

func TestInitialization(t *testing.T) {
	test.Flaky(t)
	ctx := context.Background()
	defer ctx.Done()

	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := NewQueue(ds)
	defer queue.Close()

	cids := makeCids(10)
	for _, c := range cids {
		queue.Enqueue(c)
	}

	assertOrdered(cids[:5], queue, t)

	// make a new queue, same data
	queue = NewQueue(ds)
	defer queue.Close()

	assertOrdered(cids[5:], queue, t)
}

func TestInitializationWithManyCids(t *testing.T) {
	test.Flaky(t)
	ctx := context.Background()
	defer ctx.Done()

	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := NewQueue(ds)

	cids := makeCids(25)
	for _, c := range cids {
		queue.Enqueue(c)
	}

	queue.Close()

	// make a new queue, same data
	queue = NewQueue(ds)
	defer queue.Close()

	assertOrdered(cids, queue, t)
}
