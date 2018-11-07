package files

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

// serialFile implements File, and reads from a path on the OS filesystem.
// No more than one file will be opened at a time (directories will advance
// to the next file when NextFile() is called).
type serialFile struct {
	path              string
	files             []os.FileInfo
	stat              os.FileInfo
	handleHiddenFiles bool
}

func NewSerialFile(path string, hidden bool, stat os.FileInfo) (File, error) {
	switch mode := stat.Mode(); {
	case mode.IsRegular():
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return NewReaderPathFile(path, file, stat)
	case mode.IsDir():
		// for directories, stat all of the contents first, so we know what files to
		// open when NextFile() is called
		contents, err := ioutil.ReadDir(path)
		if err != nil {
			return nil, err
		}
		return &serialFile{path, contents, stat, hidden}, nil
	case mode&os.ModeSymlink != 0:
		target, err := os.Readlink(path)
		if err != nil {
			return nil, err
		}
		return NewLinkFile(target, stat), nil
	default:
		return nil, fmt.Errorf("unrecognized file type for %s: %s", path, mode.String())
	}
}

func (f *serialFile) NextFile() (string, File, error) {
	// if a file was opened previously, close it
	err := f.Close()
	if err != nil {
		switch err2 := err.(type) {
		case *os.PathError:
			if err2.Err != os.ErrClosed {
				return "", nil, err
			}
		default:
			return "", nil, err
		}
	}

	// if there aren't any files left in the root directory, we're done
	if len(f.files) == 0 {
		return "", nil, io.EOF
	}

	stat := f.files[0]
	f.files = f.files[1:]

	for !f.handleHiddenFiles && strings.HasPrefix(stat.Name(), ".") {
		if len(f.files) == 0 {
			return "", nil, io.EOF
		}

		stat = f.files[0]
		f.files = f.files[1:]
	}

	// open the next file
	filePath := filepath.ToSlash(filepath.Join(f.path, stat.Name()))

	// recursively call the constructor on the next file
	// if it's a regular file, we will open it as a ReaderFile
	// if it's a directory, files in it will be opened serially
	sf, err := NewSerialFile(filePath, f.handleHiddenFiles, stat)
	if err != nil {
		return "", nil, err
	}

	return stat.Name(), sf, nil
}

func (f *serialFile) Close() error {
	return nil
}

func (f *serialFile) Stat() os.FileInfo {
	return f.stat
}

func (f *serialFile) Size() (int64, error) {
	if !f.stat.IsDir() {
		return f.stat.Size(), nil
	}

	var du int64
	err := filepath.Walk(f.path, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if fi != nil && fi.Mode().IsRegular() {
			du += fi.Size()
		}
		return nil
	})

	return du, err
}

var _ Directory = &serialFile{}
