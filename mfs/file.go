package mfs

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	chunker "github.com/ipfs/boxo/chunker"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	mod "github.com/ipfs/boxo/ipld/unixfs/mod"
	ipld "github.com/ipfs/go-ipld-format"
)

// File represents a file in the MFS, its logic its mainly targeted
// to coordinating (potentially many) `FileDescriptor`s pointing to
// it.
type File struct {
	inode

	// Lock to coordinate the `FileDescriptor`s associated to this file.
	desclock sync.RWMutex

	// This isn't any node, it's the root node that represents the
	// entire DAG of nodes that comprise the file.
	// TODO: Rename, there should be an explicit term for these root nodes
	// of a particular sub-DAG that abstract an upper layer's entity.
	node ipld.Node

	// Lock around the `node` that represents this file, necessary because
	// there may be many `FileDescriptor`s operating on this `File`.
	nodeLock sync.RWMutex

	RawLeaves bool
}

// NewFile returns a NewFile object with the given parameters.  If the
// Cid version is non-zero RawLeaves will be enabled.
func NewFile(name string, node ipld.Node, parent parent, dserv ipld.DAGService) (*File, error) {
	fi := &File{
		inode: inode{
			name:       name,
			parent:     parent,
			dagService: dserv,
		},
		node: node,
	}
	if node.Cid().Prefix().Version > 0 {
		fi.RawLeaves = true
	}
	return fi, nil
}

func (fi *File) Open(flags Flags) (_ FileDescriptor, _retErr error) {
	if flags.Write {
		fi.desclock.Lock()
		defer func() {
			if _retErr != nil {
				fi.desclock.Unlock()
			}
		}()
	} else if flags.Read {
		fi.desclock.RLock()
		defer func() {
			if _retErr != nil {
				fi.desclock.RUnlock()
			}
		}()
	} else {
		return nil, errors.New("file opened for neither reading nor writing")
	}

	fi.nodeLock.RLock()
	node := fi.node
	fi.nodeLock.RUnlock()

	// TODO: Move this `switch` logic outside (maybe even
	// to another package, this seems like a job of UnixFS),
	// `NewDagModifier` uses the IPLD node, we're not
	// extracting anything just doing a safety check.
	switch node := node.(type) {
	case *dag.ProtoNode:
		fsn, err := ft.FSNodeFromBytes(node.Data())
		if err != nil {
			return nil, err
		}

		switch fsn.Type() {
		default:
			return nil, errors.New("unsupported fsnode type for 'file'")
		case ft.TSymlink:
			return nil, errors.New("symlinks not yet supported")
		case ft.TFile, ft.TRaw:
			// OK case
		}
	case *dag.RawNode:
		// Ok as well.
	}

	dmod, err := mod.NewDagModifier(context.TODO(), node, fi.dagService, chunker.DefaultSplitter)
	// TODO: Remove the use of the `chunker` package here, add a new `NewDagModifier` in
	// `go-unixfs` with the `DefaultSplitter` already included.
	if err != nil {
		return nil, err
	}
	dmod.RawLeaves = fi.RawLeaves

	return &fileDescriptor{
		inode: fi,
		flags: flags,
		mod:   dmod,
		state: stateCreated,
	}, nil
}

// Size returns the size of this file
// TODO: Should we be providing this API?
// TODO: There's already a `FileDescriptor.Size()` that
// through the `DagModifier`'s `fileSize` function is doing
// pretty much the same thing as here, we should at least call
// that function and wrap the `ErrNotUnixfs` with an MFS text.
func (fi *File) Size() (int64, error) {
	fi.nodeLock.RLock()
	defer fi.nodeLock.RUnlock()
	switch nd := fi.node.(type) {
	case *dag.ProtoNode:
		fsn, err := ft.FSNodeFromBytes(nd.Data())
		if err != nil {
			return 0, err
		}
		return int64(fsn.FileSize()), nil
	case *dag.RawNode:
		return int64(len(nd.RawData())), nil
	default:
		return 0, errors.New("unrecognized node type in mfs/file.Size()")
	}
}

// GetNode returns the dag node associated with this file
// TODO: Use this method and do not access the `nodeLock` directly anywhere else.
func (fi *File) GetNode() (ipld.Node, error) {
	fi.nodeLock.RLock()
	defer fi.nodeLock.RUnlock()
	return fi.node, nil
}

// TODO: Tight coupling with the `FileDescriptor`, at the
// very least this should be an independent function that
// takes a `File` argument and automates the open/flush/close
// operations.
// TODO: Why do we need to flush a file that isn't opened?
// (the `OpenWriteOnly` seems to implicitly be targeting a
// closed file, a file we forgot to flush? can we close
// a file without flushing?)
func (fi *File) Flush() error {
	// open the file in fullsync mode
	fd, err := fi.Open(Flags{Write: true, Sync: true})
	if err != nil {
		return err
	}

	defer fd.Close()

	return fd.Flush()
}

func (fi *File) Sync() error {
	// just being able to take the writelock means the descriptor is synced
	// TODO: Why?
	fi.desclock.Lock()
	defer fi.desclock.Unlock() // Defer works around "empty critical section (SA2001)"
	return nil
}

// Type returns the type FSNode this is
func (fi *File) Type() NodeType {
	return TFile
}

func (fi *File) Mode() (os.FileMode, error) {
	fi.nodeLock.RLock()
	defer fi.nodeLock.RUnlock()

	nd, err := fi.GetNode()
	if err != nil {
		return 0, err
	}
	fsn, err := ft.ExtractFSNode(nd)
	if err != nil {
		return 0, err
	}
	return fsn.Mode() & 0xFFF, nil
}

func (fi *File) SetMode(mode os.FileMode) error {
	nd, err := fi.GetNode()
	if err != nil {
		return err
	}

	fsn, err := ft.ExtractFSNode(nd)
	if err != nil {
		if errors.Is(err, ft.ErrNotProtoNode) {
			// Wrap raw node in protonode.
			data := nd.RawData()
			return fi.setNodeData(ft.FilePBDataWithStat(data, uint64(len(data)), mode, time.Time{}))
		}
		return err
	}

	fsn.SetMode(mode)
	data, err := fsn.GetBytes()
	if err != nil {
		return err
	}

	return fi.setNodeData(data)
}

// ModTime returns the files' last modification time
func (fi *File) ModTime() (time.Time, error) {
	fi.nodeLock.RLock()
	defer fi.nodeLock.RUnlock()

	nd, err := fi.GetNode()
	if err != nil {
		return time.Time{}, err
	}
	fsn, err := ft.ExtractFSNode(nd)
	if err != nil {
		return time.Time{}, err
	}
	return fsn.ModTime(), nil
}

// SetModTime sets the files' last modification time
func (fi *File) SetModTime(ts time.Time) error {
	nd, err := fi.GetNode()
	if err != nil {
		return err
	}

	fsn, err := ft.ExtractFSNode(nd)
	if err != nil {
		if errors.Is(err, ft.ErrNotProtoNode) {
			// Wrap raw node in protonode.
			data := nd.RawData()
			return fi.setNodeData(ft.FilePBDataWithStat(data, uint64(len(data)), 0, ts))
		}
		return err
	}

	fsn.SetModTime(ts)
	data, err := fsn.GetBytes()
	if err != nil {
		return err
	}

	return fi.setNodeData(data)
}

func (fi *File) setNodeData(data []byte) error {
	nd := dag.NodeWithData(data)
	err := fi.inode.dagService.Add(context.TODO(), nd)
	if err != nil {
		return err
	}

	fi.nodeLock.Lock()
	fi.node = nd
	parent := fi.inode.parent
	name := fi.inode.name
	fi.nodeLock.Unlock()
	return parent.updateChildEntry(child{name, fi.node})
}
