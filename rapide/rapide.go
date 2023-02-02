package rapide

import (
	"context"
	"fmt"
	"io"
	mrand "math/rand"
	"sync"
	"sync/atomic"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/ipsl"
	"go.uber.org/multierr"
)

// TODO: Add ordering garentees in the API when we figureout signaling for ordering in the protocol.
type ServerDrivenDownloader interface {
	// When an error is seen on the channel, it is assumed that no more blocks will ever be received.
	// Clients are not required to perform traversal validation, the RAPIDE client will takes care of this.
	// Clients are required to validate hashes.
	Download(context.Context, cid.Cid, ipsl.Traversal) (ClosableBlockIterator, error)
}

// ClosableBlockIterator is an interator that implements io.Closer, we will always cancel the context
// and call close when stopping a request.
type ClosableBlockIterator interface {
	io.Closer
	blocks.BlockIterator
}

// A Client is a collection of routers and protocols that can be used to do requests.
type Client struct {
	ServerDrivenDownloaders []ServerDrivenDownloader
}

func (c *Client) Get(ctx context.Context, root cid.Cid, traversal ipsl.Traversal) <-chan blocks.BlockOrError {
	ctx, cancel := context.WithCancel(ctx)
	out := make(chan blocks.BlockOrError)
	d := &download{
		out:    out,
		ctx:    ctx,
		cancel: cancel,
		done:   uint64(len(c.ServerDrivenDownloaders)),
		root: node{
			state:     todo,
			workers:   uint(len(c.ServerDrivenDownloaders)),
			cid:       root,
			traversal: traversal,
		},
		errors: make([]error, len(c.ServerDrivenDownloaders)),
	}

	seedRand := mrand.New(mrand.NewSource(mrand.Int63()))
	for i, sdd := range c.ServerDrivenDownloaders {
		d.startServerDrivenWorker(ctx, sdd, &d.root, &d.errors[i], seedRand.Int63()^int64(i))
	}

	return out
}

type download struct {
	out       chan<- blocks.BlockOrError
	ctx       context.Context
	cancel    context.CancelFunc
	root      node
	errors    []error
	done      uint64
	closeOnce sync.Once
}

// err cuts out the download and make it return an error, this is intended for unrecoverable errors.
func (d *download) err(err error) {
	d.closeOnce.Do(func() {
		select {
		case d.out <- blocks.IsNot(err):
		case <-d.ctx.Done():
		}
		d.cancel()
		close(d.out)
	})
}

func (d *download) finish() {
	d.closeOnce.Do(func() {
		d.cancel()
		close(d.out)
	})
}

func (d *download) workerFinished() {
	// don't decrement d.done because if we suceeded we don't want them to attempt to return an error
	d.root.mu.Lock()
	defer d.root.mu.Unlock()
	if d.root.state == done && len(d.root.childrens) == 0 {
		d.finish() // file was downloaded !
	}
}

func (d *download) workerErrored() {
	var minusOne uint64
	minusOne--
	if atomic.AddUint64(&d.done, minusOne) == 0 {
		// we were the last worker, error
		d.err(multierr.Combine(d.errors...))
	}
}

type node struct {
	// parent is not protected by the mutex and is readonly after creation
	parent *node
	// cid is not protected by the mutex and is readonly after creation
	cid cid.Cid
	// to avoid ABBA lock ordering issues it is prohibited to grab the lock of the parent while holding the lock of the child.
	// it is also prohibited to grab the mutex of two different parts of the tree without holding all intermediary nodes for the same reason.
	mu sync.Mutex
	// traversal will be nilled out when the nodes has been explored.
	traversal ipsl.Traversal
	childrens []*node
	workers   uint
	state     nodeState
}

// expand will run the Traversal and create childrens, it must be called while holding n.mu.Mutex
func (n *node) expand(d *download, b blocks.Block) error {
	if n.state != todo {
		panic(fmt.Sprintf("expanding a node that is not todo: %d", n.state))
	}

	newResults, err := n.traversal.Traverse(b)
	if err != nil {
		d.err(err)
		return err
	}

	n.state = done
	n.traversal = nil // early gc

	childrens := make([]*node, len(newResults))
	for i, r := range newResults {
		childrens[i] = &node{
			state:     todo,
			parent:    n,
			cid:       r.Cid,
			traversal: r.Traversal,
		}
	}
	n.childrens = childrens

	for node, parent := n, n.parent; len(node.childrens) == 0; node, parent = parent, parent.parent {
		if parent == nil {
			// finished!
			d.finish()
			return io.EOF
		}

		// nothing to do, backtrack
		parent.mu.Lock()
		for i, v := range parent.childrens {
			if v != node {
				continue
			}

			childrens := append(parent.childrens[:i], parent.childrens[i+1:]...)
			parent.childrens = append(childrens, nil)[:len(childrens)] // null out for gc
			break
		}
		parent.mu.Unlock()
	}

	return nil
}

// n.state - notStarted = the number of runners
type nodeState uint

const (
	_ nodeState = iota
	// done indicates that the current node has been downloaded but it doesn't indicates that this part of the tree is complete
	// we remove completed parts of the tree from the lists.
	done
	// todo indicates that the node should be downloaded, a node that is in progress will still showup in todo
	// but it will have a non zero amount of workers
	todo
)
