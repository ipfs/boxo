package rapide

import (
	"context"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/ipsl"
)

type serverDrivenWorker struct {
	impl     ServerDrivenDownloader
	download *download
	outErr   *error
	current  *node
	tasks    map[cid.Cid]*node
	rand     mrand.Rand

	// TODO: add a dontGoThere map which tells you what part of the dag this node is not able to handle
}

func (d *download) startServerDrivenWorker(ctx context.Context, impl ServerDrivenDownloader, root *node, outErr *error, seed int64) {
	w := &serverDrivenWorker{
		impl:     impl,
		download: d,
		outErr:   outErr,
		current:  root,
		tasks:    make(map[cid.Cid]*node),
		rand:     *mrand.New(mrand.NewSource(seed)),
	}

	go w.work(ctx)
}

func (w *serverDrivenWorker) work(ctx context.Context) {
	for {
		workCid, traversal, ok := w.findWork()
		if !ok {
			w.resetAllCurrentNodesWorkState()
			w.download.workerFinished()
			return // finished
		}

		tasks := w.tasks
		for k := range tasks {
			delete(tasks, k)
		}
		tasks[workCid] = w.current

		err := w.doOneDownload(ctx, workCid, traversal)
		switch err {
		case nil, io.EOF, errGotDoneBlock:
			w.resetCurrentChildsNodeWorkState()
			continue
		default:
			// FIXME: support ignoring erroring parts of the tree when searching (dontGoThere)
			// If the error is that some blocks are not available, we should backtrack and find more work.
			w.resetAllCurrentNodesWorkState()
			*w.outErr = err
			w.download.workerErrored()
			return
		}
	}
}

var errUnexpectedBlock = errors.New("got an unexpected block")
var errGotDoneBlock = errors.New("downloaded an already done node")

// doOneDownload will return nil when it does not find work
func (w *serverDrivenWorker) doOneDownload(ctx context.Context, workCid cid.Cid, traversal ipsl.Traversal) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := w.impl.Download(ctx, workCid, traversal)
	if err != nil {
		// FIXME: support ignoring erroring parts of the tree when searching
		// If the error is that some blocks are not available, we should backtrack and find more work.
		return err
	}
	defer stream.Close()

	for {
		if len(w.tasks) == 0 {
			return nil
		}
		b, err := stream.Next()
		switch err {
		case nil:
		case io.EOF:
			// FIXME: check why len(w.tasks) == 0 is sometime false here
			return err
		default:
			return err
		}

		select {
		case w.download.out <- blocks.Is(b):
		case <-ctx.Done():
			w.download.err(ctx.Err())
		}

		c := b.Cid()
		task, ok := w.tasks[c]
		if !ok {
			// received unexpected block
			return errUnexpectedBlock
		}
		delete(w.tasks, c)

		task.mu.Lock()
		if task.state == done {
			task.mu.Unlock()
			// we finished all parts of our tree, cancel current work and restart a new request.
			return errGotDoneBlock
		}
		if err := task.expand(w.download, b); err != nil {
			task.mu.Unlock()
			return err
		}

	Switch:
		switch len(task.childrens) {
		case 0:
			// terminated node, remove them (and all removed parents from our task list)
			for len(task.childrens) == 0 {
				task.mu.Unlock()
				delete(w.tasks, task.cid)
				task = task.parent
				if !w.isOurTask(task) {
					break Switch
				}
				task.mu.Lock()
			}
			task.mu.Unlock()
		default:
			// add new work we discovered
			for _, child := range task.childrens {
				child.mu.Lock()
				if child.state == todo {
					child.workers += 1
					child.mu.Unlock()
					w.tasks[child.cid] = child
				} else {
					child.mu.Unlock()
				}
			}
			task.mu.Unlock()
		}
	}
}

func (w *serverDrivenWorker) findWork() (cid.Cid, ipsl.Traversal, bool) {
	// repeat until we find more work
	c := w.current
	for {
		if c == nil {
			// we are finished!
			w.current = nil
			return cid.Cid{}, nil, false
		}
		c.mu.Lock()
		switch c.state {
		case 0:
			c.mu.Unlock()
			panic("zero state on node") // unreachable
		case todo:
			// start this node
			traversal := c.traversal
			c.workers += 1
			c.mu.Unlock()
			w.current = c
			return c.cid, traversal, true

			// TODO: add smart racing support, someone is already taking care of this, we should backtrack
		case done:
			// first search in it's childs if it has something we could run
			if c != w.current {
				c.workers += 1
			}
			var minWorkers, luck uint
			var min *node
			for _, child := range c.childrens {
				// we run a minimum search, we want the node that have the least amount of workers currently
				// TODO: filter childs in the dontGoThere map
				minWorkers, min, luck = w.compareChildWithMinimums(child, minWorkers, min, luck)
			}
			if min != nil {
				c.mu.Unlock()
				c = min
				continue
			}

			// this node is fully completed, do backtracking
			c.workers -= 1
			new := c.parent
			c.mu.Unlock()
			c = new
			continue
		default:
			s := c.state
			c.mu.Unlock()
			panic(fmt.Sprintf("unkown node state: %d", s))
		}
	}
}

func (w *serverDrivenWorker) compareChildWithMinimums(child *node, minWorkers uint, min *node, luck uint) (uint, *node, uint) {
	child.mu.Lock()
	defer child.mu.Unlock()

	switch {
	case min == nil:
		minWorkers = child.workers
		min = child
	case child.workers < minWorkers:
		minWorkers = child.workers
		min = child
		luck = 0
	case child.workers == minWorkers:
		// if scores are identical randomly select other nodes to randomly distribute where downloads are placed
		if luck == 0 {
			// lazy initialisation of luck, this allows to creating a random value when better values exists back to back
			luck = uint(w.rand.Int())
		}
		newLuck := uint(w.rand.Int())
		if newLuck >= luck {
			break
		}
		minWorkers = child.workers
		min = child
		luck = newLuck
	}

	return minWorkers, min, luck
}

// resetCurrentChildsNodeWorkState updates the state of childs of the current node to longer count towards it.
func (w *serverDrivenWorker) resetCurrentChildsNodeWorkState() {
	c := w.current
	if c == nil {
		return // nothing to do
	}

	// recursively walk the state and remove ourself from counters
	// This is pretty contensius but that should be fine because server driven downloads should be cancel rarely, also most of thoses are gonna go on the fast path anyway.	c.mu.Lock()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, child := range c.childrens {
		w.recurseCancelNode(child)
	}
}

// resetAllCurrentNodesWorkState updates the state of the current node to longer count towards it.
func (w *serverDrivenWorker) resetAllCurrentNodesWorkState() {
	c := w.current
	if c == nil {
		return // nothing to do
	}

	// recursively walk the state and remove ourself from counters
	// This is pretty contensius but that should be fine because server driven downloads should be cancel rarely, also most of thoses are gonna go on the fast path anyway.	c.mu.Lock()
	w.recurseCancelNode(c)
}

func (w *serverDrivenWorker) recurseCancelNode(c *node) {
	if !w.isOurTask(c) {
		return // not our task
	}

	// This is pretty contensius but that should be fine because server driven downloads should be cancel rarely, also most of thoses are gonna go on the fast path anyway.	c.mu.Lock()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers -= 1
	for _, child := range c.childrens {
		w.recurseCancelNode(child)
	}
}

func (w *serverDrivenWorker) isOurTask(c *node) bool {
	if task, ok := w.tasks[c.cid]; !ok {
		return task == c
	}
	return false
}
