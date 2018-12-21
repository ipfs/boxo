package mfs

import (
	"fmt"
	"io"

	mod "github.com/ipfs/go-unixfs/mod"

	context "context"
)

// One `File` can have many `FileDescriptor`s associated to it
// (only one if it's RW, many if they are RO, see `File.desclock`).
// A `FileDescriptor` contains the "view" of the file (through an
// instance of a `DagModifier`), that's why it (and not the `File`)
// has the responsibility to `Flush` (which crystallizes that view
// in the `File`'s `Node`).
type FileDescriptor interface {
	io.Reader
	CtxReadFull(context.Context, []byte) (int, error)

	io.Writer
	io.WriterAt

	io.Closer
	io.Seeker

	Truncate(int64) error
	Size() (int64, error)
	Flush() error
}

type fileDescriptor struct {
	inode      *File
	mod        *mod.DagModifier
	perms      int
	sync       bool
	hasChanges bool

	// TODO: Where is this variable set?
	closed bool
}

// Size returns the size of the file referred to by this descriptor
func (fi *fileDescriptor) Size() (int64, error) {
	return fi.mod.Size()
}

// Truncate truncates the file to size
func (fi *fileDescriptor) Truncate(size int64) error {
	if fi.perms == OpenReadOnly {
		return fmt.Errorf("cannot call truncate on readonly file descriptor")
	}
	fi.hasChanges = true
	return fi.mod.Truncate(size)
}

// Write writes the given data to the file at its current offset
func (fi *fileDescriptor) Write(b []byte) (int, error) {
	if fi.perms == OpenReadOnly {
		return 0, fmt.Errorf("cannot write on not writeable descriptor")
	}
	fi.hasChanges = true
	return fi.mod.Write(b)
}

// Read reads into the given buffer from the current offset
func (fi *fileDescriptor) Read(b []byte) (int, error) {
	if fi.perms == OpenWriteOnly {
		return 0, fmt.Errorf("cannot read on write-only descriptor")
	}
	return fi.mod.Read(b)
}

// Read reads into the given buffer from the current offset
func (fi *fileDescriptor) CtxReadFull(ctx context.Context, b []byte) (int, error) {
	if fi.perms == OpenWriteOnly {
		return 0, fmt.Errorf("cannot read on write-only descriptor")
	}
	return fi.mod.CtxReadFull(ctx, b)
}

// Close flushes, then propogates the modified dag node up the directory structure
// and signals a republish to occur
func (fi *fileDescriptor) Close() error {
	defer func() {
		switch fi.perms {
		case OpenReadOnly:
			fi.inode.desclock.RUnlock()
		case OpenWriteOnly, OpenReadWrite:
			fi.inode.desclock.Unlock()
		}
		// TODO: `closed` should be set here.
	}()

	if fi.closed {
		panic("attempted to close file descriptor twice!")
	}

	if fi.hasChanges {
		err := fi.mod.Sync()
		if err != nil {
			return err
		}

		fi.hasChanges = false

		// explicitly stay locked for flushUp call,
		// it will manage the lock for us
		return fi.flushUp(fi.sync)
	}

	return nil
}

// Flush generates a new version of the node of the underlying
// UnixFS directory (adding it to the DAG service) and updates
// the entry in the parent directory (setting `fullSync` to
// propagate the update all the way to the root).
func (fi *fileDescriptor) Flush() error {
	return fi.flushUp(true)
}

// flushUp syncs the file and adds it to the dagservice
// it *must* be called with the File's lock taken
// If `fullSync` is set the changes are propagated upwards
// (the `Up` part of `flushUp`).
func (fi *fileDescriptor) flushUp(fullSync bool) error {
	nd, err := fi.mod.GetNode()
	if err != nil {
		return err
	}

	err = fi.inode.dagService.Add(context.TODO(), nd)
	if err != nil {
		return err
	}
	// TODO: Very similar logic to the update process in
	// `Directory`, the logic should be unified, both structures
	// (`File` and `Directory`) are backed by a IPLD node with
	// a UnixFS format that is the actual target of the update
	// (regenerating it and adding it to the DAG service).

	fi.inode.nodeLock.Lock()
	fi.inode.node = nd
	// TODO: Create a `SetNode` method.
	name := fi.inode.name
	parent := fi.inode.parent
	// TODO: Can the parent be modified? Do we need to do this inside the lock?
	fi.inode.nodeLock.Unlock()
	// TODO: Maybe all this logic should happen in `File`.

	if fullSync {
		return parent.updateChildEntry(child{name, nd})
	}

	return nil
}

// Seek implements io.Seeker
func (fi *fileDescriptor) Seek(offset int64, whence int) (int64, error) {
	return fi.mod.Seek(offset, whence)
}

// Write At writes the given bytes at the offset 'at'
func (fi *fileDescriptor) WriteAt(b []byte, at int64) (int, error) {
	if fi.perms == OpenReadOnly {
		return 0, fmt.Errorf("cannot write on not writeable descriptor")
	}
	fi.hasChanges = true
	return fi.mod.WriteAt(b, at)
}
