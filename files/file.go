package files

import (
	"errors"
	"io"
	"os"
)

var (
	ErrNotDirectory = errors.New("couldn't call NextFile(), this isn't a directory")
	ErrNotReader    = errors.New("this file is a directory, can't use Reader functions")

	ErrNotSupported = errors.New("operation not supported")
)

// File is an interface that provides functionality for handling
// files/directories as values that can be supplied to commands. For
// directories, child files are accessed serially by calling `NextFile()`
//
// Read/Seek methods are only valid for files
// NextFile method is only valid for directories
type File interface {
	io.Reader
	io.Closer
	io.Seeker

	// Size returns size of the
	Size() (int64, error)

	// IsDirectory returns true if the File is a directory (and therefore
	// supports calling `Files`/`Walk`) and false if the File is a normal file
	// (and therefore supports calling `Read`/`Close`/`Seek`)
	IsDirectory() bool

	// NextFile returns the next child file available (if the File is a
	// directory). It will return io.EOF if no more files are
	// available. If the file is a regular file (not a directory), NextFile
	// will return a non-nil error.
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
