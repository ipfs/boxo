package files

import (
	"bytes"
	"io"
	"io/ioutil"
	"sort"
)

// ToFile is an alias for n.(File). If the file isn't a regular file, nil value
// will be returned
func ToFile(n Node) File {
	f, _ := n.(File)
	return f
}

// ToDir is an alias for n.(Directory). If the file isn't directory, a nil value
// will be returned
func ToDir(n Node) Directory {
	d, _ := n.(Directory)
	return d
}

// FileFrom is a convenience function which tries to extract or create new file
// from provided value. If a passed value can't be turned into a File, nil will
// be returned.
//
// Supported types:
// * files.File (cast from Node)
// * DirEntry / DirIterator (cast from e.Node())
// * []byte (wrapped into NewReaderFile)
// * io.Reader / io.ReadCloser (wrapped into NewReaderFile)
func FileFrom(n interface{}) File {
	switch f := n.(type) {
	case File:
		return f
	case DirEntry:
		return ToFile(f.Node())
	case []byte:
		return NewReaderFile(ioutil.NopCloser(bytes.NewReader(f)), nil)
	case io.ReadCloser:
		return NewReaderFile(f, nil)
	case io.Reader:
		return NewReaderFile(ioutil.NopCloser(f), nil)
	default:
		return nil
	}
}

// DirFrom is a convenience function which tries to extract or create new
// directory from the provided value. If a passed value can't be turned into a
// Directory, nil will be returned.
//
// Supported types:
// * files.File (cast from Node)
// * DirEntry (cast from e.Node())
// * DirIterator (current file, cast from e.Node())
// * []DirEntry (wrapped into NewSliceFile)
// * map[string]Node (wrapped into NewSliceFile)
func DirFrom(n interface{}) Directory {
	switch f := n.(type) {
	case Directory:
		return f
	case DirEntry:
		return ToDir(f.Node())
	case []DirEntry:
		return NewSliceFile(f)
	case map[string]Node:
		ents := make([]DirEntry, 0, len(f))
		for name, nd := range f {
			ents = append(ents, FileEntry(name, nd))
		}
		sort.Slice(ents, func(i, j int) bool {
			return ents[i].Name() < ents[j].Name()
		})
		return NewSliceFile(ents)
	default:
		return nil
	}
}
