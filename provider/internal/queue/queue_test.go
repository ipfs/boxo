package queue

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-test/random"
)

func assertOrdered(cids []cid.Cid, q *Queue, t *testing.T) {
	t.Helper()

	for i, c := range cids {
		select {
		case dequeued, ok := <-q.Dequeue():
			if !ok {
				t.Fatal("queue closed")
			}
			if c != dequeued {
				t.Fatalf("Error in ordering of CID %d retrieved from queue. Expected: %s, got: %s", i, c, dequeued)
			}

		case <-time.After(time.Second):
			t.Fatal("Timeout waiting for cids to be provided.")
		}
	}
}

func TestBasicOperation(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := New(ds)
	defer queue.Close()

	cids := random.Cids(10)
	for _, c := range cids {
		queue.Enqueue(c)
	}

	assertOrdered(cids, queue, t)

	err := queue.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err = queue.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMangledData(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := New(ds)
	defer queue.Close()

	cids := random.Cids(10)
	for _, c := range cids {
		queue.Enqueue(c)
	}

	// put bad data in the queue
	queueKey := datastore.NewKey("/test/0")
	err := queue.ds.Put(context.Background(), queueKey, []byte("borked"))
	if err != nil {
		t.Fatal(err)
	}

	// expect to only see the valid cids we entered
	expected := cids
	assertOrdered(expected, queue, t)
}

func TestInitialization(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := New(ds)
	defer queue.Close()

	cids := random.Cids(10)
	for _, c := range cids {
		queue.Enqueue(c)
	}

	assertOrdered(cids[:5], queue, t)

	err := queue.Close()
	if err != nil {
		t.Fatal(err)
	}

	// make a new queue, same data
	queue = New(ds)
	defer queue.Close()

	assertOrdered(cids[5:], queue, t)
}

func TestInitializationWithManyCids(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := New(ds)
	defer queue.Close()

	cids := random.Cids(25)
	for _, c := range cids {
		queue.Enqueue(c)
	}

	err := queue.Close()
	if err != nil {
		t.Fatal(err)
	}

	// make a new queue, same data
	queue = New(ds)
	defer queue.Close()

	assertOrdered(cids, queue, t)
}
