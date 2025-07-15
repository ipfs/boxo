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

func TestDeduplicateCids(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := New(ds)
	defer queue.Close()

	cids := random.Cids(5)
	queue.Enqueue(cids[0])
	queue.Enqueue(cids[0])
	queue.Enqueue(cids[1])
	queue.Enqueue(cids[2])
	queue.Enqueue(cids[1])
	queue.Enqueue(cids[3])
	queue.Enqueue(cids[0])
	queue.Enqueue(cids[4])

	assertOrdered(cids, queue, t)
}

func TestClear(t *testing.T) {
	const cidCount = 25

	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue := New(ds)
	defer queue.Close()

	for _, c := range random.Cids(cidCount) {
		queue.Enqueue(c)
	}

	// Cause queued entried to be saved in datastore.
	err := queue.Close()
	if err != nil {
		t.Fatal(err)
	}

	queue = New(ds)
	defer queue.Close()

	for _, c := range random.Cids(cidCount) {
		queue.Enqueue(c)
	}

	rmCount := queue.Clear()
	t.Log("Cleared", rmCount, "entries from provider queue")
	if rmCount != 2*cidCount {
		t.Fatalf("expected %d cleared, got %d", 2*cidCount, rmCount)
	}

	if err = queue.Close(); err != nil {
		t.Fatal(err)
	}

	// Ensure no data when creating new queue.
	queue = New(ds)
	defer queue.Close()

	select {
	case <-queue.Dequeue():
		t.Fatal("dequeue should not return")
	case <-time.After(10 * time.Millisecond):
	}
}
