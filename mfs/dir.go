package mfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	dag "github.com/ipfs/go-merkledag"
	ft "github.com/ipfs/go-unixfs"
	uio "github.com/ipfs/go-unixfs/io"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

var ErrNotYetImplemented = errors.New("not yet implemented")
var ErrInvalidChild = errors.New("invalid child node")
var ErrDirExists = errors.New("directory already has entry by that name")

// TODO: There's too much functionality associated with this structure,
// let's organize it (and if possible extract part of it elsewhere)
// and document the main features of `Directory` here.
type Directory struct {
	inode

	// Cache.
	// TODO: Should this be a single cache of `FSNode`s?
	childDirs map[string]*Directory
	files     map[string]*File

	lock sync.Mutex
	// TODO: What content is being protected here exactly? The entire directory?

	ctx context.Context

	// UnixFS directory implementation used for creating,
	// reading and editing directories.
	unixfsDir uio.Directory

	modTime time.Time
}

// NewDirectory constructs a new MFS directory.
//
// You probably don't want to call this directly. Instead, construct a new root
// using NewRoot.
func NewDirectory(ctx context.Context, name string, node ipld.Node, parent mutableParent, dserv ipld.DAGService) (*Directory, error) {
	db, err := uio.NewDirectoryFromNode(dserv, node)
	if err != nil {
		return nil, err
	}

	return &Directory{
		inode: inode{
			name:       name,
			parent:     parent,
			dagService: dserv,
		},
		ctx:       ctx,
		unixfsDir: db,
		childDirs: make(map[string]*Directory),
		files:     make(map[string]*File),
		modTime:   time.Now(),
	}, nil
}

// GetCidBuilder gets the CID builder of the root node
func (d *Directory) GetCidBuilder() cid.Builder {
	return d.unixfsDir.GetCidBuilder()
}

// SetCidBuilder sets the CID builder
func (d *Directory) SetCidBuilder(b cid.Builder) {
	d.unixfsDir.SetCidBuilder(b)
}

// This method implements the `mutableParent` interface. It first updates
// the child entry in the underlying UnixFS directory and then if `fullSync`
// is set it saves the new content through the internal DAG service. Then,
// also if `fullSync` is set, it propagates the update to its parent (through
// this same interface) with the new node already updated with the new entry.
// So, `fullSync` entails operations at two different layers:
//   1. DAG: save the newly created directory node with the updated entry.
//   2. MFS: propagate the update upwards repeating the whole process in the
//           parent.
func (d *Directory) updateChildEntry(c child, fullSync bool) error {

	// There's a local flush (`closeChildUpdate`) and a propagated flush (`updateChildEntry`).

	newDirNode, err := d.closeChildUpdate(c, fullSync)
	if err != nil {
		return err
	}

	// TODO: The `sync` seems to be tightly coupling this two pieces of code,
	// we use the node returned by `closeChildUpdate` (which entails a copy)
	// only if `sync` is set, and we are discarding it otherwise. At the very
	// least the `if sync {` clause at the end of `closeChildUpdate` should
	// be merged with this one (the use of the `lock` is stopping this at the
	// moment, re-evaluate when its purpose has been better understood).

	if fullSync {
		return d.parent.updateChildEntry(child{d.name, newDirNode}, true)
		// Setting `fullSync` to true here means, if the original child that
		// initiated the update process wanted to propagate it upwards then
		// continue to do so all the way up to the root, that is, the only
		// time `fullSync` can be false is in the first call (which will be
		// the *only* call), we either update the first parent entry or *all*
		// the parent's.
	}

	return nil
}

// This method implements the part of `updateChildEntry` that needs
// to be locked around: in charge of updating the UnixFS layer and
// generating the new node reflecting the update.
func (d *Directory) closeChildUpdate(c child, fullSync bool) (*dag.ProtoNode, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	err := d.updateChild(c)
	if err != nil {
		return nil, err
	}
	// TODO: Clearly define how are we propagating changes to lower layers
	// like UnixFS.

	if fullSync {
		return d.flushCurrentNode()
	}
	return nil, nil
}

// Recreate the underlying UnixFS directory node and save it in the DAG layer.
func (d *Directory) flushCurrentNode() (*dag.ProtoNode, error) {
	nd, err := d.unixfsDir.GetNode()
	if err != nil {
		return nil, err
	}

	err = d.dagService.Add(d.ctx, nd)
	if err != nil {
		return nil, err
	}
	// TODO: This method is called in `closeChildUpdate` while the lock is
	// taken, we need the lock while operating on `unixfsDir` to create the
	// new node but do we also need to keep the lock while adding it to the
	// DAG service? Evaluate refactoring these two methods together and better
	// redistributing the node.

	pbnd, ok := nd.(*dag.ProtoNode)
	if !ok {
		return nil, dag.ErrNotProtobuf
	}
	// TODO: Why do we check the node *after* adding it to the DAG service?

	return pbnd.Copy().(*dag.ProtoNode), nil
	// TODO: Why do we need a copy?
}

// Update child entry in the underlying UnixFS directory.
func (d *Directory) updateChild(c child) error {
	err := d.AddUnixFSChild(c)
	if err != nil {
		return err
	}

	d.modTime = time.Now()

	return nil
}

func (d *Directory) Type() NodeType {
	return TDir
}

// childNode returns a FSNode under this directory by the given name if it exists.
// it does *not* check the cached dirs and files
func (d *Directory) childNode(name string) (FSNode, error) {
	nd, err := d.childFromDag(name)
	if err != nil {
		return nil, err
	}

	return d.cacheNode(name, nd)
}

// cacheNode caches a node into d.childDirs or d.files and returns the FSNode.
func (d *Directory) cacheNode(name string, nd ipld.Node) (FSNode, error) {
	switch nd := nd.(type) {
	case *dag.ProtoNode:
		fsn, err := ft.FSNodeFromBytes(nd.Data())
		if err != nil {
			return nil, err
		}

		switch fsn.Type() {
		case ft.TDirectory, ft.THAMTShard:
			ndir, err := NewDirectory(d.ctx, name, nd, d, d.dagService)
			if err != nil {
				return nil, err
			}

			d.childDirs[name] = ndir
			return ndir, nil
		case ft.TFile, ft.TRaw, ft.TSymlink:
			nfi, err := NewFile(name, nd, d, d.dagService)
			if err != nil {
				return nil, err
			}
			d.files[name] = nfi
			return nfi, nil
		case ft.TMetadata:
			return nil, ErrNotYetImplemented
		default:
			return nil, ErrInvalidChild
		}
	case *dag.RawNode:
		nfi, err := NewFile(name, nd, d, d.dagService)
		if err != nil {
			return nil, err
		}
		d.files[name] = nfi
		return nfi, nil
	default:
		return nil, fmt.Errorf("unrecognized node type in cache node")
	}
}

// Child returns the child of this directory by the given name
func (d *Directory) Child(name string) (FSNode, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.childUnsync(name)
}

func (d *Directory) Uncache(name string) {
	d.lock.Lock()
	defer d.lock.Unlock()
	delete(d.files, name)
	delete(d.childDirs, name)
	// TODO: We definitely need to join these maps if we are manipulating
	// them like this.
}

// childFromDag searches through this directories dag node for a child link
// with the given name
func (d *Directory) childFromDag(name string) (ipld.Node, error) {
	return d.unixfsDir.Find(d.ctx, name)
}

// childUnsync returns the child under this directory by the given name
// without locking, useful for operations which already hold a lock
func (d *Directory) childUnsync(name string) (FSNode, error) {
	cdir, ok := d.childDirs[name]
	if ok {
		return cdir, nil
	}

	cfile, ok := d.files[name]
	if ok {
		return cfile, nil
	}

	return d.childNode(name)
}

type NodeListing struct {
	Name string
	Type int
	Size int64
	Hash string
}

func (d *Directory) ListNames(ctx context.Context) ([]string, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	var out []string
	err := d.unixfsDir.ForEachLink(ctx, func(l *ipld.Link) error {
		out = append(out, l.Name)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (d *Directory) List(ctx context.Context) ([]NodeListing, error) {
	var out []NodeListing
	err := d.ForEachEntry(ctx, func(nl NodeListing) error {
		out = append(out, nl)
		return nil
	})
	return out, err
}

func (d *Directory) ForEachEntry(ctx context.Context, f func(NodeListing) error) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.unixfsDir.ForEachLink(ctx, func(l *ipld.Link) error {
		c, err := d.childUnsync(l.Name)
		if err != nil {
			return err
		}

		nd, err := c.GetNode()
		if err != nil {
			return err
		}

		child := NodeListing{
			Name: l.Name,
			Type: int(c.Type()),
			Hash: nd.Cid().String(),
		}

		if c, ok := c.(*File); ok {
			size, err := c.Size()
			if err != nil {
				return err
			}
			child.Size = size
		}

		return f(child)
	})
}

func (d *Directory) Mkdir(name string) (*Directory, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	fsn, err := d.childUnsync(name)
	if err == nil {
		switch fsn := fsn.(type) {
		case *Directory:
			return fsn, os.ErrExist
		case *File:
			return nil, os.ErrExist
		default:
			return nil, fmt.Errorf("unrecognized type: %#v", fsn)
		}
	}

	ndir := ft.EmptyDirNode()
	ndir.SetCidBuilder(d.GetCidBuilder())

	err = d.dagService.Add(d.ctx, ndir)
	if err != nil {
		return nil, err
	}

	err = d.AddUnixFSChild(child{name, ndir})
	if err != nil {
		return nil, err
	}

	dirobj, err := NewDirectory(d.ctx, name, ndir, d, d.dagService)
	if err != nil {
		return nil, err
	}

	d.childDirs[name] = dirobj
	return dirobj, nil
}

func (d *Directory) Unlink(name string) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	delete(d.childDirs, name)
	delete(d.files, name)

	return d.unixfsDir.RemoveChild(d.ctx, name)
}

func (d *Directory) Flush() error {
	nd, err := d.GetNode()
	if err != nil {
		return err
	}

	return d.parent.updateChildEntry(child{d.name, nd}, true)
}

// AddChild adds the node 'nd' under this directory giving it the name 'name'
func (d *Directory) AddChild(name string, nd ipld.Node) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	_, err := d.childUnsync(name)
	if err == nil {
		return ErrDirExists
	}

	err = d.dagService.Add(d.ctx, nd)
	if err != nil {
		return err
	}

	err = d.AddUnixFSChild(child{name, nd})
	if err != nil {
		return err
	}

	d.modTime = time.Now()
	return nil
}

// AddUnixFSChild adds a child to the inner UnixFS directory
// and transitions to a HAMT implementation if needed.
func (d *Directory) AddUnixFSChild(c child) error {
	if uio.UseHAMTSharding {
		// If the directory HAMT implementation is being used and this
		// directory is actually a basic implementation switch it to HAMT.
		if basicDir, ok := d.unixfsDir.(*uio.BasicDirectory); ok {
			hamtDir, err := basicDir.SwitchToSharding(d.ctx)
			if err != nil {
				return err
			}
			d.unixfsDir = hamtDir
		}
	}

	err := d.unixfsDir.AddChild(d.ctx, c.Name, c.Node)
	if err != nil {
		return err
	}

	return nil
}

// TODO: Difference between `sync` and `Flush`? This seems
// to be related to the internal cache and not to the MFS
// hierarchy update.
func (d *Directory) sync() error {
	for name, dir := range d.childDirs {
		nd, err := dir.GetNode()
		if err != nil {
			return err
		}

		err = d.updateChild(child{name, nd})
		if err != nil {
			return err
		}
	}

	for name, file := range d.files {
		nd, err := file.GetNode()
		if err != nil {
			return err
		}

		err = d.updateChild(child{name, nd})
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Directory) Path() string {
	cur := d
	var out string
	for cur != nil {
		switch parent := cur.parent.(type) {
		case *Directory:
			out = path.Join(cur.name, out)
			cur = parent
		case *Root:
			return "/" + out
		default:
			panic("directory parent neither a directory nor a root")
		}
	}
	return out
}

func (d *Directory) GetNode() (ipld.Node, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	err := d.sync()
	if err != nil {
		return nil, err
	}

	nd, err := d.unixfsDir.GetNode()
	if err != nil {
		return nil, err
	}

	err = d.dagService.Add(d.ctx, nd)
	if err != nil {
		return nil, err
	}

	return nd.Copy(), err
}
