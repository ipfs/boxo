// unixfs provides type safe low level premitives to read and write unixfs blocks.
// It handles encoding, decoding and validation but does not handle any
// cross-block linking, this is provided by various opiniated implementations
// available in sub packages or as an exercise to the consumer.
//
// This package is Data-Oriented, the main way this impact tradeoffs is that
// state is moved to control flow when possible and allocations are hammered to
// a minimum for example by returning pointers aliased to the input.
package unixfs

import (
	"errors"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
)

// Entry is a basic unit block.
type Entry struct {
	Cid cid.Cid
	// tSize encode the comulative size of the DAG.
	// the zero value indicates tsize is missing.
	tSize uint64
}

func (e Entry) TSize() (tsize uint64, ok bool) {
	if e.tSize == 0 {
		return 0, false
	}

	return e.tSize - 1, true
}

func (e Entry) Untyped() Entry {
	return e
}

var _ Node = File{}

type File struct {
	badge
	Entry
	Data      []byte
	Childrens []FileEntry
}

func FileEntryWithTSize(c cid.Cid, fileSize, tSize uint64) FileEntry {
	return FileEntry{Entry: Entry{Cid: c, tSize: tSize + 1}, FileSize: fileSize}
}

type FileEntry struct {
	Entry
	// FileSize is the logical size of the file at this location once decoded.
	FileSize uint64
}

var _ Node = Directory{}

type Directory struct {
	badge
	Entry
	Childrens []DirectoryEntry
}

type DirectoryEntry struct {
	Entry
	Name AliasableString
}

var _ Node = Symlink{}

type Symlink struct {
	badge
	Entry
	Value []byte
}

// badge authorize a type to be a [Node].
// If you add a new type using this you need to update [Parse].
type badge struct{}

func (badge) nodeBadge() {
	panic("badge was called even tho it only exists as a way to trick the type checker")
}

// Node is an interface that can exclusively be a [File], [Directory] or [Symlink]. We might add more in the future.
// You MUST NOT embed this interface, it's only purpose is to provide type safe enums.
type Node interface {
	// Untyped returns the untyped [Entry] for that value stripped of all type related information.
	Untyped() Entry
	// nodeBadge must never be called it's just here to trick the type checker.
	nodeBadge()
}

// Parse it provides a type safe solution to Decode using the badged interface [Node].
// [File.Data], [DirectoryEntry.Name] and [Symlink.Value] values are aliased to b.RawData().
// The data argument MUST hash to cid, this wont check the validaty of the hash.
func Parse(b blocks.Block) (Node, error) {
	switch t, f, d, s, err := ParseAppend(nil, nil, b.Cid(), b.RawData()); t {
	case TError:
		return nil, err
	case TFile:
		return f, nil
	case TDirectory:
		return d, nil
	case TSymlink:
		return s, nil
	default:
		return nil, errors.New("unknown node type in Parse (Should never happen please open an issue !): " + t.String())
	}
}

// ParseAppend is like [Parse] except it is turbo charged to avoid allocation.
// It returns a [Type] which indicates which of the struct is correct, all of this is passed on the stack or registers.
// Assuming the capacity in the slices are big enough and err == nil it does not allocate anything, arguments do not escape.
// [File.Data], [DirectoryEntry.Name] and [Symlink.Value] values are aliased to b.RawData().
// It also accepts the input slices which will be append to and returned in structs to avoid allocations.
// It is only ever gonna clobber the slice related to the type of data decoded.
// It only ever clobber extra capacity within the slices, it may do so in the case of an error.
// The data argument MUST hash to cid, this wont check the validaty of the hash.
func ParseAppend(fileChildrens []FileEntry, directoryChildrens []DirectoryEntry, cid cid.Cid, data []byte) (t Type, f File, d Directory, s Symlink, err error) {
	// Avoid clobbering the used part of the slice.
	fileChildrens = fileChildrens[len(fileChildrens):]
	directoryChildrens = directoryChildrens[len(directoryChildrens):]

	pref := cid.Prefix()
	switch c := multicodec.Code(pref.Codec); c {
	case multicodec.Raw:
		t = TFile
		f = File{
			Entry: Entry{
				Cid:   cid,
				tSize: uint64(len(data)) + 1,
			},
			Data:      data,
			Childrens: fileChildrens,
		}
		return
	default:
		err = errors.New("unsupported codec: " + c.String())
		return
	}
}
