// package mfs implements an in memory model of a mutable IPFS filesystem.
// TODO: Develop on this line (and move it to `doc.go`).

package mfs

import (
	"context"
	"errors"
	"fmt"
	"time"

	dag "github.com/ipfs/go-merkledag"
	ft "github.com/ipfs/go-unixfs"

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

// This interface represents the basic property of MFS directories of updating
// children entries with modified content. Implemented by both the MFS
// `Directory` and `Root` (which is basically a `Directory` with republishing
// support).
//
// TODO: What is `fullsync`? (unnamed `bool` argument)
// TODO: There are two types of persistence/flush that need to be
// distinguished here, one at the DAG level (when I store the modified
// nodes in the DAG service) and one in the UnixFS/MFS level (when I modify
// the entry/link of the directory that pointed to the modified node).
type mutableParent interface {
	// Method called by a child to its parent to signal to update the content
	// pointed to in the entry by that child's name. The child sends as
	// arguments its own information (under the `child` structure) and a flag
	// (`fullsync`) indicating whether or not to propagate the update upwards:
	// modifying a directory entry entails modifying its contents which means
	// that its parent (the parent's parent) will also need to be updated (and
	// so on).
	updateChildEntry(c child, fullSync bool) error
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

// updateChildEntry implements the mutableParent interface, and signals to the publisher that
// there are changes ready to be published.
// This is the only thing that separates a `Root` from a `Directory`.
// TODO: Evaluate merging both.
// TODO: The `sync` argument isn't used here (we've already reached
// the top), document it and maybe make it an anonymous variable (if
// that's possible).
func (kr *Root) updateChildEntry(c child, fullSync bool) error {
	err := kr.GetDirectory().dagService.Add(context.TODO(), c.Node)
	if err != nil {
		return err
	}
	// TODO: Why are we not using the inner directory lock nor
	// applying the same procedure as `Directory.updateChildEntry`?

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
