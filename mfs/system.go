// package mfs implements an in memory model of a mutable IPFS filesystem.
// TODO: Develop on this line (and move it elsewhere), delete the rest.
//
// It consists of four main structs:
// 1) The Filesystem
//        The filesystem serves as a container and entry point for various mfs filesystems
// 2) Root
//        Root represents an individual filesystem mounted within the mfs system as a whole
// 3) Directories
// 4) Files
package mfs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	dag "github.com/ipfs/go-merkledag"
	ft "github.com/ipfs/go-unixfs"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
)

// TODO: Remove if not used.
var ErrNotExist = errors.New("no such rootfs")

var log = logging.Logger("mfs")

// TODO: Remove if not used.
var ErrIsDirectory = errors.New("error: is a directory")

// The information that an MFS `Directory` has about its children
// when updating one of its entries: when a child mutates it signals
// its parent directory to update its entry (under `Name`) with the
// new content (in `Node`).
type child struct {
	Name string
	Node ipld.Node
}

// TODO: Rename (avoid "close" terminology, if anything
// we are persisting/flushing changes).
// This is always a directory (since we are referring to the parent),
// can be an intermediate directory in the filesystem or the `Root`.
// TODO: What is `fullsync`? (unnamed `bool` argument)
// TODO: There are two types of persistence/flush that need to be
// distinguished here, one at the DAG level (when I store the modified
// nodes in the DAG service) and one in the UnixFS/MFS level (when I modify
// the entry/link of the directory that pointed to the modified node).
type childCloser interface {
	closeChild(child, bool) error
}

type NodeType int

const (
	TFile NodeType = iota
	TDir
)

// FSNode represents any node (directory, or file) in the MFS filesystem.
// Not to be confused with the `unixfs.FSNode`.
type FSNode interface {
	GetNode() (ipld.Node, error)
	Flush() error
	Type() NodeType
}

// IsDir checks whether the FSNode is dir type
func IsDir(fsn FSNode) bool {
	return fsn.Type() == TDir
}

// IsFile checks whether the FSNode is file type
func IsFile(fsn FSNode) bool {
	return fsn.Type() == TFile
}

// Root represents the root of a filesystem tree.
type Root struct {

	// Root directory of the MFS layout.
	dir *Directory

	repub *Republisher
}

// NewRoot creates a new Root and starts up a republisher routine for it.
func NewRoot(parent context.Context, ds ipld.DAGService, node *dag.ProtoNode, pf PubFunc) (*Root, error) {

	var repub *Republisher
	if pf != nil {
		repub = NewRepublisher(parent, pf, time.Millisecond*300, time.Second*3)
		repub.setVal(node.Cid())
		go repub.Run()
	}

	root := &Root{
		repub: repub,
	}

	fsn, err := ft.FSNodeFromBytes(node.Data())
	if err != nil {
		log.Error("IPNS pointer was not unixfs node")
		// TODO: IPNS pointer?
		return nil, err
	}

	switch fsn.Type() {
	case ft.TDirectory, ft.THAMTShard:
		newDir, err := NewDirectory(parent, node.String(), node, root, ds)
		if err != nil {
			return nil, err
		}

		root.dir = newDir
	case ft.TFile, ft.TMetadata, ft.TRaw:
		return nil, fmt.Errorf("root can't be a file (unixfs type: %s)", fsn.Type())
		// TODO: This special error reporting case doesn't seem worth it, we either
		// have a UnixFS directory or we don't.
	default:
		return nil, fmt.Errorf("unrecognized unixfs type: %s", fsn.Type())
	}
	return root, nil
}

// GetDirectory returns the root directory.
func (kr *Root) GetDirectory() *Directory {
	return kr.dir
}

// Flush signals that an update has occurred since the last publish,
// and updates the Root republisher.
// TODO: We are definitely abusing the "flush" terminology here.
func (kr *Root) Flush() error {
	nd, err := kr.GetDirectory().GetNode()
	if err != nil {
		return err
	}

	if kr.repub != nil {
		kr.repub.Update(nd.Cid())
	}
	return nil
}

// FlushMemFree flushes the root directory and then uncaches all of its links.
// This has the effect of clearing out potentially stale references and allows
// them to be garbage collected.
// CAUTION: Take care not to ever call this while holding a reference to any
// child directories. Those directories will be bad references and using them
// may have unintended racy side effects.
// A better implemented mfs system (one that does smarter internal caching and
// refcounting) shouldnt need this method.
// TODO: Review the motivation behind this method once the cache system is
// refactored.
func (kr *Root) FlushMemFree(ctx context.Context) error {
	dir := kr.GetDirectory()

	if err := dir.Flush(); err != nil {
		return err
	}

	dir.lock.Lock()
	defer dir.lock.Unlock()

	for name := range dir.files {
		delete(dir.files, name)
	}
	for name := range dir.childDirs {
		delete(dir.childDirs, name)
	}
	// TODO: Can't we just create new maps?

	return nil
}

// closeChild implements the childCloser interface, and signals to the publisher that
// there are changes ready to be published.
// This is the only thing that separates a `Root` from a `Directory`.
// TODO: Evaluate merging both.
// TODO: The `sync` argument isn't used here (we've already reached
// the top), document it and maybe make it an anonymous variable (if
// that's possible).
func (kr *Root) closeChild(c child, sync bool) error {
	err := kr.GetDirectory().dagService.Add(context.TODO(), c.Node)
	if err != nil {
		return err
	}

	if kr.repub != nil {
		kr.repub.Update(c.Node.Cid())
	}
	return nil
}

func (kr *Root) Close() error {
	nd, err := kr.GetDirectory().GetNode()
	if err != nil {
		return err
	}

	if kr.repub != nil {
		kr.repub.Update(nd.Cid())
		return kr.repub.Close()
	}

	return nil
}

// TODO: Separate the remaining code in another file: `repub.go`.

// PubFunc is the function used by the `publish()` method.
type PubFunc func(context.Context, cid.Cid) error

// Republisher manages when to publish a given entry.
type Republisher struct {
	TimeoutLong  time.Duration
	TimeoutShort time.Duration
	Publish      chan struct{}
	pubfunc      PubFunc
	pubnowch     chan chan struct{}

	ctx    context.Context
	cancel func()

	lk      sync.Mutex
	val     cid.Cid
	lastpub cid.Cid
}

// NewRepublisher creates a new Republisher object to republish the given root
// using the given short and long time intervals.
func NewRepublisher(ctx context.Context, pf PubFunc, tshort, tlong time.Duration) *Republisher {
	ctx, cancel := context.WithCancel(ctx)
	return &Republisher{
		TimeoutShort: tshort,
		TimeoutLong:  tlong,
		Publish:      make(chan struct{}, 1),
		pubfunc:      pf,
		pubnowch:     make(chan chan struct{}),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (p *Republisher) setVal(c cid.Cid) {
	p.lk.Lock()
	defer p.lk.Unlock()
	p.val = c
}

// WaitPub Returns immediately if `lastpub` value is consistent with the
// current value `val`, else will block until `val` has been published.
func (p *Republisher) WaitPub() {
	p.lk.Lock()
	consistent := p.lastpub == p.val
	p.lk.Unlock()
	if consistent {
		return
	}

	wait := make(chan struct{})
	p.pubnowch <- wait
	<-wait
}

func (p *Republisher) Close() error {
	err := p.publish(p.ctx)
	p.cancel()
	return err
}

// Touch signals that an update has occurred since the last publish.
// Multiple consecutive touches may extend the time period before
// the next Publish occurs in order to more efficiently batch updates.
func (np *Republisher) Update(c cid.Cid) {
	np.setVal(c)
	select {
	case np.Publish <- struct{}{}:
	default:
	}
}

// Run is the main republisher loop.
// TODO: Document according to:
// https://github.com/ipfs/go-ipfs/issues/5092#issuecomment-398524255.
func (np *Republisher) Run() {
	for {
		select {
		case <-np.Publish:
			quick := time.After(np.TimeoutShort)
			longer := time.After(np.TimeoutLong)

		wait:
			var pubnowresp chan struct{}

			select {
			case <-np.ctx.Done():
				return
			case <-np.Publish:
				quick = time.After(np.TimeoutShort)
				goto wait
			case <-quick:
			case <-longer:
			case pubnowresp = <-np.pubnowch:
			}

			err := np.publish(np.ctx)
			if pubnowresp != nil {
				pubnowresp <- struct{}{}
			}
			if err != nil {
				log.Errorf("republishRoot error: %s", err)
			}

		case <-np.ctx.Done():
			return
		}
	}
}

// publish calls the `PubFunc`.
func (np *Republisher) publish(ctx context.Context) error {
	np.lk.Lock()
	topub := np.val
	np.lk.Unlock()

	err := np.pubfunc(ctx, topub)
	if err != nil {
		return err
	}
	np.lk.Lock()
	np.lastpub = topub
	np.lk.Unlock()
	return nil
}
