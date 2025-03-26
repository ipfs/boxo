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
	batchSize           = 1024
	batchCommitInterval = 5 * time.Second

	// Number of input CIDs to buffer without blocking.
	inputBufferSize = 1024 * 256
	// Time for Close to wait to finish writing CIDs to datastore.
	shutdownTimeout = 30 * time.Second
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
	ds        datastore.Batching
	dequeue   chan cid.Cid
	enqueue   chan cid.Cid
	inBuf     *chanqueue.ChanQueue[cid.Cid] // in-memory queue to buffer input
	close     context.CancelFunc
	closed    chan struct{}
	closeOnce sync.Once

	syncDone  chan struct{}
	syncMutex sync.Mutex
}

// NewQueue creates a queue for cids
func NewQueue(ds datastore.Batching) *Queue {
	dequeue := make(chan cid.Cid)
	enqueue := make(chan cid.Cid)

	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		close:    cancel,
		closed:   make(chan struct{}),
		ds:       namespace.Wrap(ds, datastore.NewKey("/queue")),
		dequeue:  dequeue,
		enqueue:  enqueue,
		syncDone: make(chan struct{}, 1),
	}

	q.inBuf = chanqueue.New(
		chanqueue.WithInput(enqueue),
		chanqueue.WithCapacity[cid.Cid](inputBufferSize),
	)
	go q.worker(ctx)

	return q
}

func (q *Queue) Sync() {
	q.syncMutex.Lock()
	q.inBuf.In() <- cid.Undef
	<-q.syncDone
	q.syncMutex.Unlock()
}

// Close stops the queue
func (q *Queue) Close() error {
	var err error
	q.closeOnce.Do(func() {
		// Close input queue and wait for worker to finish reading it.
		if q.ds == nil {
			q.inBuf.Shutdown()
		} else {
			q.inBuf.Close()
			select {
			case <-q.closed:
			case <-time.After(shutdownTimeout):
				q.close() // force immediate shutdown
				<-q.closed
				err = fmt.Errorf("provider queue: %d cids not written to datastore", q.inBuf.Len())
			}
			close(q.dequeue) // no more output from this queue
		}
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
	defer q.inBuf.Shutdown()

	var (
		c       cid.Cid = cid.Undef
		counter uint64
		cstr    string
		k       datastore.Key = datastore.Key{}
	)
	readInBuf := q.inBuf.Out()

	var batchCount int
	b, err := q.ds.Batch(ctx)
	if err != nil {
		log.Errorf("Failed to create batch, stopping provider: %s", err)
		return
	}

	defer func() {
		if batchCount != 0 {
			if err := b.Commit(ctx); err != nil {
				log.Errorf("Failed to write cid batch: %s", err)
			}
		}
		close(q.syncDone)
	}()

	batchTicker := time.NewTicker(batchCommitInterval)
	defer batchTicker.Stop()

	for {
		//fmt.Println("---> inbuf len:", q.inBuf.Len(), "batch count:", batchCount)
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
		var commit, needSync bool

		select {
		case toQueue, ok := <-readInBuf:
			if !ok {
				return
			}
			if toQueue == cid.Undef {
				if batchCount != 0 {
					commit = true
				}
				needSync = true
				break
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

			//if err := q.ds.Put(ctx, nextKey, toQueue.Bytes()); err != nil {
			if err := b.Put(ctx, nextKey, toQueue.Bytes()); err != nil {
				log.Errorf("Failed to batch cid: %s", err)
				continue
			}
			batchCount++
			if batchCount == batchSize {
				commit = true
			}
		case <-batchTicker.C:
			if batchCount != 0 && q.inBuf.Len() == 0 {
				commit = true
			}
		case dequeue <- c:
			// Do not batch delete. Delete must be committed immediately, otherwise the same head cid will be read from the datastore.
			err := q.ds.Delete(ctx, k)
			if err != nil {
				log.Errorf("Failed to delete queued cid %s with key %s: %s", c, k, err)
				continue
			}
			c = cid.Undef
		case <-ctx.Done():
			return
		}

		if commit {
			if err = b.Commit(ctx); err != nil {
				log.Errorf("Failed to write cid batch, stopping provider: %s", err)
				return
			}
			b, err = q.ds.Batch(ctx)
			if err != nil {
				log.Errorf("Failed to create batch, stopping provider: %s", err)
				return
			}
			batchCount = 0
			commit = false
		}

		if needSync {
			needSync = false
			select {
			case q.syncDone <- struct{}{}:
			case <-ctx.Done():
				return
			}
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
