package files

import (
	"errors"
	"io"
	"os"
)

var (
	ErrNotDirectory = errors.New("file isn't a directory")
	ErrNotReader    = errors.New("file isn't a regular file")

	ErrNotSupported = errors.New("operation not supported")
)

// File is an interface that provides functionality for handling
// files/directories as values that can be supplied to commands. For
// directories, child files are accessed serially by calling `NextFile()`
//
// Read/Seek methods are only valid for files
// NextFile method is only valid for directories
type File interface {
	io.Closer

	// Size returns size of this file (if this file is a directory, total size of
	// all files stored in the tree should be returned). Some implementations may
	// choose not to implement this
	Size() (int64, error)
}

// Regular represents a regular Unix file
type Regular interface {
	File

	io.Reader
	io.Seeker
}

// DirEntry exposes information about a directory entry
type DirEntry interface {
	// Name returns the base name of this entry, which is the base name of
	// the referenced file
	Name() string

	// File returns the file referenced by this DirEntry
	File() File

	// Regular is an alias for ent.File().(Regular). If the file isn't a regular
	// file, nil value will be returned
	Regular() Regular

	// Dir is an alias for ent.File().(directory). If the file isn't a directory,
	// nil value will be returned
	Dir() Directory
}

// DirIterator is a iterator over directory entries.
// See Directory.Entries for more
type DirIterator interface {
	// DirEntry holds information about current directory entry.
	// Note that after creating new iterator you MUST call Next() at least once
	// before accessing these methods. Calling these methods without prior calls
	// to Next() and after Next() returned false may result in undefined behavior
	DirEntry

	// Next advances the iterator to the next file.
	Next() bool

	// Err may return an error after the previous call to Next() returned `false`.
	// If the previous call to Next() returned `true`, Err() is guaranteed to
	// return nil
	Err() error
}

// Directory is a special file which can link to any number of files.
type Directory interface {
	File

	// Entries returns a stateful iterator over directory entries.
	//
	// Example usage:
	//
	// it := dir.Entries()
	// for it.Next() {
	//   name := it.Name()
	//   file := it.File()
	//   [...]
	// }
	// if it.Err() != nil {
	//   return err
	// }
	//
	// Note:
	// - Below limitations aren't applicable to all implementations, consult
	//   your implementations manual before using this interface in a way that
	//   doesn't meet these constraints
	// - Some implementations may only allow reading in order - so if the iterator
	//   returns a directory you must iterate over it's entries first before
	//   calling Next again
	// - Order is not guaranteed
	// - Depending on implementation it may not be safe to iterate multiple
	//   'branches' in parallel
	Entries() (DirIterator, error)
}

// FileInfo exposes information on files in local filesystem
type FileInfo interface {
	File

	// AbsPath returns full real file path.
	AbsPath() string

	// Stat returns os.Stat of this file, may be nil for some files
	Stat() os.FileInfo
}
