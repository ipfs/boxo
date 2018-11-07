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

// Regular represents the regular Unix file
type Regular interface {
	File

	io.Reader
	io.Seeker
}

// Directory is a special file which can link to any number of files
type Directory interface {
	File

	// NextFile returns the next child file available (if the File is a
	// directory). It will return io.EOF if no more files are
	// available.
	//
	// Note:
	// - Some implementations may only allow reading in order - if a
	//   child directory is returned, you need to read all it's children
	//   first before calling NextFile on parent again. Before doing parallel
	//   reading or reading entire level at once, make sure the implementation
	//   you are using allows that
	// - Returned files may not be sorted
	// - Depending on implementation it may not be safe to iterate multiple
	//   children in parallel
	NextFile() (string, File, error)
}

// FileInfo exposes information on files in local filesystem
type FileInfo interface {
	File

	// AbsPath returns full real file path.
	AbsPath() string

	// Stat returns os.Stat of this file
	Stat() os.FileInfo
}
