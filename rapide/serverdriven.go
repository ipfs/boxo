package rapide

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/ipsl"
)

type serverDrivenWorker struct {
	impl     ServerDrivenDownloader
	download *download

	current *node

	tasks map[cid.Cid]*node

	// TODO: add a dontGoThere map which tells you what part of the dag this node is not able to handle
}

func (d *download) startServerDrivenWorker(ctx context.Context, impl ServerDrivenDownloader, root *node) {
	go (&serverDrivenWorker{
		impl:     impl,
		download: d,
		current:  root,
		tasks:    make(map[cid.Cid]*node),
	}).work(ctx)
}

func (w *serverDrivenWorker) work(ctx context.Context) {
	defer w.download.workerFinished()
	defer w.resetCurrentChildsNodeWorkState()

workLoop:
	for {
		workCid, traversal, ok := w.findWork()
		if !ok {
			return // finished
		}

		tasks := w.tasks
		for k := range tasks {
			delete(tasks, k)
		}
		tasks[workCid] = w.current

		ctx, cancelCurrentRequest := context.WithCancel(ctx)
		stream, err := w.impl.Download(ctx, workCid, traversal)
		if err != nil {
			// FIXME: support ignoring erroring parts of the tree when searching
			// If the error is that some blocks are not available, we should backtrack and find more work.
			cancelCurrentRequest()
			return
		}

		for {
			if len(tasks) == 0 {
				cancelCurrentRequest()
				w.resetCurrentChildsNodeWorkState()
				continue workLoop
			}
			b, err := stream.Next()
			if err != nil {
				switch {
				case errors.Is(err, context.Canceled):
					cancelCurrentRequest()
					return // request canceled
				case err == io.EOF:
					cancelCurrentRequest()
					w.resetCurrentChildsNodeWorkState()
					continue workLoop
				default:
					// FIXME: support ignoring erroring parts of the tree when searching (dontGoThere)
					// If the error is that some blocks are not available, we should backtrack and find more work.
					cancelCurrentRequest()
					return
				}
			}

			select {
			case w.download.out <- blocks.Is(b):
			case <-ctx.Done():
				w.download.err(ctx.Err())
			}

			c := b.Cid()
			task, ok := tasks[c]
			if !ok {
				// received unexpected block
				cancelCurrentRequest()
				return
			}

			task.mu.Lock()
			if task.state == done {
				task.mu.Unlock()
				// we finished all parts of our tree, cancel current work and restart a new request.
				cancelCurrentRequest()
				w.resetCurrentChildsNodeWorkState()
				continue workLoop
			}
			if err := task.expand(w.download, b); err != nil {
				task.mu.Unlock()
				cancelCurrentRequest()
				w.resetCurrentChildsNodeWorkState()
				return
			}

		Switch:
			switch len(task.childrens) {
			case 0:
				// terminated node, remove them (and all removed parents from our task list)
				for len(task.childrens) == 0 {
					task.mu.Unlock()
					delete(tasks, task.cid)
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
						tasks[child.cid] = child
					} else {
						child.mu.Unlock()
					}
				}
				task.mu.Unlock()
			}
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
			if c.workers == 0 {
				traversal := c.traversal
				c.workers += 1
				c.mu.Unlock()
				w.current = c
				return c.cid, traversal, true
			}

			// someone is already taking care of this, backtrack
			// TODO: add racing support
			c.workers -= 1
			c.mu.Unlock()
			c = c.parent
			continue
		case done:
			// first search in it's childs if it has something we could run
			c.workers += 1
			var minWorkers uint
			var min *node
			for _, child := range c.childrens {
				// we run a minimum search, we want the node that have the least amount of workers currently
				// TODO: filter childs in the dontGoThere map
				child.mu.Lock()
				switch {
				case min == nil:
					minWorkers = child.workers
					min = child
				case child.workers < minWorkers:
					minWorkers = child.workers
					min = child
				}
				child.mu.Unlock()
			}
			if min != nil {
				c.mu.Unlock()
				c = min
				continue
			}

			// this node is fully completed, backtracking
			// TODO: add c in dontGoThere (we failed to select any child)
			c.workers -= 1
			new := c.parent
			c.mu.Unlock()
			c = new
			continue
		default:
			c.mu.Unlock()
			panic(fmt.Sprintf("unkown node state: %d", c.state))
		}
	}
}

// resetCurrentChildsNodeWorkState updates the state of the current node to longer count towards it.
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
