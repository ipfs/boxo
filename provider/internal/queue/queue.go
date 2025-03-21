package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gammazero/chanqueue"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	namespace "github.com/ipfs/go-datastore/namespace"
	query "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("provider.queue")

const (
	// Number of input CIDs to buffer without blocking. If <= 1, then use channel.
	inputBufferSize = 65536
	// Time for Close to wait to finish writing CIDs to datastore.
	shutdownTimeout = 5 * time.Second
)

// Queue provides a FIFO interface to the datastore for storing cids.
//
// Cids in the process of being provided when a crash or shutdown occurs may be
// in the queue when the node is brought back online depending on whether they
// were fully written to the underlying datastore.
//
// Input to the queue is buffered in memory, up to inputBufferSize, to increase
// the speed of data onboarding. This input buffer behaves as a channel with a
// very large capacity.
type Queue struct {
	// used to differentiate queues in datastore
	// e.g. provider vs reprovider
	ds        datastore.Datastore // Must be threadsafe
	dequeue   chan cid.Cid
	enqueue   chan cid.Cid
	inBuf     *chanqueue.ChanQueue[cid.Cid] // in-memory queue to buffer input
	close     context.CancelFunc
	closed    chan struct{}
	closeOnce sync.Once
}

// NewQueue creates a queue for cids
func NewQueue(ds datastore.Datastore) *Queue {
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		ds:      namespace.Wrap(ds, datastore.NewKey("/queue")),
		dequeue: make(chan cid.Cid),
		close:   cancel,
		closed:  make(chan struct{}),
	}
	if inputBufferSize > 1 {
		q.enqueue = make(chan cid.Cid)
		q.inBuf = chanqueue.New(
			chanqueue.WithInput(q.enqueue),
			chanqueue.WithCapacity[cid.Cid](inputBufferSize),
		)
	} else {
		q.enqueue = make(chan cid.Cid, inputBufferSize)
	}

	go q.worker(ctx)
	return q
}

// Close stops the queue
func (q *Queue) Close() error {
	var err error
	q.closeOnce.Do(func() {
		// Close input queue and wait for worker to finish reading it.
		close(q.enqueue)
		select {
		case <-q.closed:
		case <-time.After(shutdownTimeout):
			q.close() // force immediate shutdown
			<-q.closed
			err = fmt.Errorf("provider queue: %d cids not written to datastore", q.inBuf.Len())
		}
		close(q.dequeue) // no more output from this queue
	})
	return err
}

// Enqueue puts a cid in the queue
func (q *Queue) Enqueue(cid cid.Cid) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("failed to enqueue CID: shutting down")
		}
	}()
	q.enqueue <- cid
	return
}

// Dequeue returns a channel that for reading entries from the queue,
func (q *Queue) Dequeue() <-chan cid.Cid {
	return q.dequeue
}

// worker run dequeues and enqueues when available.
func (q *Queue) worker(ctx context.Context) {
	defer close(q.closed)

	var k datastore.Key = datastore.Key{}
	var c cid.Cid = cid.Undef
	var cstr string
	var counter uint64

	var readInBuf <-chan cid.Cid
	if q.inBuf != nil {
		readInBuf = q.inBuf.Out()
	} else {
		readInBuf = q.enqueue
	}

	for {
		if c == cid.Undef {
			head, err := q.getQueueHead(ctx)
			if err != nil {
				log.Errorf("error querying for head of queue: %s, stopping provider", err)
				return
			}
			if head != nil {
				k = datastore.NewKey(head.Key)
				c, err = cid.Parse(head.Value)
				if err != nil {
					log.Warnf("error parsing queue entry cid with key (%s), removing it from queue: %s", head.Key, err)
					if err = q.ds.Delete(ctx, k); err != nil {
						log.Errorf("error deleting queue entry with key (%s), due to error (%s), stopping provider", head.Key, err)
						return
					}
					continue
				}
			} // else queue is empty
			cstr = c.String()
		}

		// If c != cid.Undef set dequeue and attempt write, otherwise wait for enqueue
		var dequeue chan cid.Cid
		if c != cid.Undef {
			dequeue = q.dequeue
		}

		select {
		case toQueue, ok := <-readInBuf:
			if !ok {
				return
			}
			// Add suffix to key path to allow multiple entries with same sequence.
			nextKey := datastore.NewKey(fmt.Sprintf("%020d/%s", counter, cstr))
			counter++

			if c == cid.Undef {
				// fast path, skip rereading the datastore if we don't have anything in hand yet
				c = toQueue
				k = nextKey
				cstr = c.String()
			}

			if err := q.ds.Put(ctx, nextKey, toQueue.Bytes()); err != nil {
				log.Errorf("Failed to enqueue cid: %s", err)
				continue
			}
		case dequeue <- c:
			err := q.ds.Delete(ctx, k)
			if err != nil {
				log.Errorf("Failed to delete queued cid %s with key %s: %s", c, k, err)
				continue
			}
			c = cid.Undef
		case <-ctx.Done():
			if q.inBuf != nil {
				for range readInBuf {
				}
			}
			return
		}
	}
}

func (q *Queue) getQueueHead(ctx context.Context) (*query.Entry, error) {
	qry := query.Query{
		Orders: []query.Order{query.OrderByKey{}},
		Limit:  1,
	}
	results, err := q.ds.Query(ctx, qry)
	if err != nil {
		return nil, err
	}
	defer results.Close()
	r, ok := results.NextSync()
	if !ok {
		return nil, nil
	}

	return &r.Entry, r.Error
}
