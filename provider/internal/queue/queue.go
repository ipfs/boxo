package queue

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gammazero/deque"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	namespace "github.com/ipfs/go-datastore/namespace"
	query "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("provider.queue")

const (
	// batchSize is the limit on number of CIDs kept in memory at which ther
	// are all written to the datastore.o
	batchSize = 16 * 1024
	// batchCommitInterval is the sime since the last batch commit to write all
	// CIDs remaining in memory.
	batchCommitInterval = 2 * time.Minute
	// shutdownTimeout is the duration that Close waits to finish writing CIDs
	// to the datastore.
	shutdownTimeout = 20 * time.Second
)

// Queue provides a FIFO interface to the datastore for storing cids.
//
// CIDs in the process of being provided when a crash or shutdown occurs may be
// in the queue when the node is brought back online depending on whether they
// were fully written to the underlying datastore.
//
// Input to the queue is buffered in memory, up to batchSize. When the input
// buffer contains batchSize items, or when batchCommitInterval has elapsed
// since the previous batch commit, the contents of the input buffer are
// written to the datastore.
type Queue struct {
	close     context.CancelFunc
	closed    chan error
	closeOnce sync.Once
	dequeue   chan cid.Cid
	ds        datastore.Batching
	enqueue   chan cid.Cid
}

// New creates a queue for cids.
func New(ds datastore.Batching) *Queue {
	ctx, cancel := context.WithCancel(context.Background())

	q := &Queue{
		close:   cancel,
		closed:  make(chan error, 1),
		dequeue: make(chan cid.Cid),
		ds:      namespace.Wrap(ds, datastore.NewKey("/queue")),
		enqueue: make(chan cid.Cid),
	}

	go q.worker(ctx)

	return q
}

// Close stops the queue.
func (q *Queue) Close() error {
	var err error
	q.closeOnce.Do(func() {
		// Close input queue and wait for worker to finish reading it.
		close(q.enqueue)
		select {
		case <-q.closed:
		case <-time.After(shutdownTimeout):
			q.close() // force immediate shutdown
			err = <-q.closed
		}
		close(q.dequeue) // no more output from this queue
	})
	return err
}

// Enqueue puts a cid in the queue.
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

func makeCidString(c cid.Cid) string {
	data := c.Bytes()
	return base64.RawURLEncoding.EncodeToString(data[len(data)-6:])
}

func makeKey(c cid.Cid, counter uint64) datastore.Key {
	return datastore.NewKey(fmt.Sprintf("%020d/%s", counter, makeCidString(c)))
}

// worker run dequeues and enqueues when available.
func (q *Queue) worker(ctx context.Context) {
	defer close(q.closed)

	var (
		c       cid.Cid
		counter uint64
		k       datastore.Key = datastore.Key{}
		inBuf   deque.Deque[cid.Cid]
	)

	const baseCap = 1024
	inBuf.SetBaseCap(baseCap)

	defer func() {
		if c != cid.Undef {
			if err := q.ds.Put(ctx, k, c.Bytes()); err != nil {
				log.Errorf("Failed to add cid for addition to batch: %s", err)
			}
			counter++
		}
		if inBuf.Len() != 0 {
			err := q.commitInput(ctx, counter, &inBuf)
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Error(err)
				if inBuf.Len() != 0 {
					q.closed <- fmt.Errorf("provider queue: %d cids not written to datastore", inBuf.Len())
				}
			}
		}
	}()

	var (
		commit  bool
		dsEmpty bool
		err     error
	)

	readInBuf := q.enqueue

	batchTimer := time.NewTimer(batchCommitInterval)
	defer batchTimer.Stop()

	for {
		if c == cid.Undef {
			if !dsEmpty {
				head, err := q.getQueueHead(ctx)
				if err != nil {
					log.Errorf("error querying for head of queue: %s, stopping provider", err)
					return
				}
				if head != nil {
					k = datastore.NewKey(head.Key)
					if err = q.ds.Delete(ctx, k); err != nil {
						log.Errorf("Failed to delete queued cid %s with key %s: %s", c, k, err)
						continue
					}
					c, err = cid.Parse(head.Value)
					if err != nil {
						log.Warnf("error parsing queue entry cid with key (%s), removing it from queue: %s", head.Key, err)
						if err = q.ds.Delete(ctx, k); err != nil {
							log.Errorf("error deleting queue entry with key (%s), due to error (%s), stopping provider", head.Key, err)
							return
						}
						continue
					}
				} else {
					dsEmpty = true
				}
			}
			if dsEmpty && inBuf.Len() != 0 {
				// There were no queued CIDs in the datastore, so read one from
				// the input buffer.
				c = inBuf.PopFront()
				k = makeKey(c, counter)
			}
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

			if c == cid.Undef {
				// Use this CID as the next output since there was nothing in
				// the datastore or buffer previously.
				c = toQueue
				k = makeKey(c, counter)
				continue
			}

			inBuf.PushBack(toQueue)
			if inBuf.Len() >= batchSize {
				commit = true
			}
		case dequeue <- c:
			c = cid.Undef
		case <-batchTimer.C:
			if inBuf.Len() != 0 {
				commit = true
			} else {
				batchTimer.Reset(batchCommitInterval)
				if inBuf.Cap() > baseCap {
					inBuf = deque.Deque[cid.Cid]{}
					inBuf.SetBaseCap(baseCap)
				}
			}
		case <-ctx.Done():
			return
		}

		if commit {
			commit = false
			n := inBuf.Len()
			err = q.commitInput(ctx, counter, &inBuf)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Errorf("%w, stopping provider", err)
				}
				return
			}
			counter += uint64(n)
			dsEmpty = false
			batchTimer.Reset(batchCommitInterval)
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

func (q *Queue) commitInput(ctx context.Context, counter uint64, cids *deque.Deque[cid.Cid]) error {
	b, err := q.ds.Batch(ctx)
	if err != nil {
		return fmt.Errorf("Failed to create batch: %w", err)
	}

	cstr := makeCidString(cids.Front())
	n := cids.Len()
	for i := 0; i < n; i++ {
		c := cids.At(i)
		key := datastore.NewKey(fmt.Sprintf("%020d/%s", counter, cstr))
		if err = b.Put(ctx, key, c.Bytes()); err != nil {
			log.Errorf("Failed to add cid for addition to batch: %s", err)
			continue
		}
		counter++
	}
	cids.Clear()

	if err = b.Commit(ctx); err != nil {
		return fmt.Errorf("failed to write to datastore: %w", err)
	}

	return nil
}
