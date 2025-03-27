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
	batchSize           = 16384
	batchCommitInterval = 5 * time.Second

	// Number of input CIDs to buffer without blocking. This capacity is only
	// used when writing batches to the datastore takes some time.
	inputBufferSize = 1024 * 64
	// Time for Close to wait to finish writing CIDs to datastore.
	shutdownTimeout = 20 * time.Second
)

// Queue provides a FIFO interface to the datastore for storing cids.
//
// Cids in the process of being provided when a crash or shutdown occurs may be
// in the queue when the node is brought back online depending on whether they
// were fully written to the underlying datastore.
//
// Input to the queue is buffered in memory, up to inputBufferSize, to maintain
// the speed at which input is consumed, even if persisting it to the datastore
// becomes slow. This input buffer behaves as a channel with a dynamic
// capacity.
type Queue struct {
	close     context.CancelFunc
	closed    chan struct{}
	closeOnce sync.Once
	dequeue   chan cid.Cid
	ds        datastore.Batching
	inBuf     *chanqueue.ChanQueue[cid.Cid]
}

// NewQueue creates a queue for cids
func NewQueue(ds datastore.Batching) *Queue {
	ctx, cancel := context.WithCancel(context.Background())

	q := &Queue{
		close:   cancel,
		closed:  make(chan struct{}),
		dequeue: make(chan cid.Cid),
		ds:      namespace.Wrap(ds, datastore.NewKey("/queue")),
		inBuf:   chanqueue.New(chanqueue.WithCapacity[cid.Cid](inputBufferSize)),
	}

	go q.worker(ctx)

	return q
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
	q.inBuf.In() <- cid
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

	b, err := q.ds.Batch(ctx)
	if err != nil {
		log.Errorf("Failed to create batch, stopping provider: %s", err)
		return
	}

	var batchCount int
	defer func() {
		if batchCount != 0 {
			if err := b.Commit(ctx); err != nil {
				log.Errorf("Failed to write cid batch: %s", err)
			}
		}
	}()

	refreshBatch := func(ctx context.Context) error {
		if batchCount == 0 {
			return nil
		}
		err := b.Commit(ctx)
		if err != nil {
			return fmt.Errorf("failed to write cid batch: %w", err)
		}
		b, err = q.ds.Batch(ctx)
		if err != nil {
			return fmt.Errorf("failed to create batch: %w", err)
		}
		batchCount = 0
		return nil
	}

	var (
		counter    uint64
		c, lastCid cid.Cid
		commit     bool
		cstr       string
		k          datastore.Key = datastore.Key{}
	)

	readInBuf := q.inBuf.Out()

	batchTicker := time.NewTicker(batchCommitInterval)
	defer batchTicker.Stop()

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
			} else if batchCount != 0 {
				// There were no queued CIDs in the datastore, but there were
				// some waiting to be written. Write them and re-read from
				// datastore.
				if err = refreshBatch(ctx); err != nil {
					if !errors.Is(err, context.Canceled) {
						log.Errorf("%w, stopping provider", err)
					}
					return
				}
				continue
			}
			cstr = c.String()
		}

		// If c != cid.Undef set dequeue and attempt write.
		var dequeue chan cid.Cid
		if c != cid.Undef {
			dequeue = q.dequeue
		}

		select {
		case toQueue, ok := <-readInBuf:
			if !ok {
				return
			}
			if toQueue == lastCid {
				continue
			}
			lastCid = toQueue

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
			if err = b.Put(ctx, nextKey, toQueue.Bytes()); err != nil {
				log.Errorf("Failed to batch cid: %s", err)
				continue
			}
			batchCount++
			if batchCount == batchSize {
				commit = true
			}
		case <-batchTicker.C:
			commit = q.inBuf.Len() == 0
		case dequeue <- c:
			// Commit current batch first so that if CID being read is still in
			// the uncommitted batch, that CID is written and deleted from the
			// datastore.
			if batchCount != 0 {
				if err = refreshBatch(ctx); err != nil {
					if !errors.Is(err, context.Canceled) {
						log.Errorf("%w, stopping provider", err)
					}
					return
				}
			}

			// Do not batch delete. Delete must be committed immediately,
			// otherwise the same head cid will be read from the datastore.
			if err = q.ds.Delete(ctx, k); err != nil {
				log.Errorf("Failed to delete queued cid %s with key %s: %s", c, k, err)
				continue
			}
			c = cid.Undef
		case <-ctx.Done():
			return
		}

		if commit {
			commit = false

			if err = refreshBatch(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Errorf("%w, stopping provider", err)
				}
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
