package files

import (
	"errors"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/url"
	"path"
	"strings"
)

const (
	multipartFormdataType = "multipart/form-data"
	multipartMixedType    = "multipart/mixed"

	applicationDirectory = "application/x-directory"
	applicationSymlink   = "application/symlink"
	applicationFile      = "application/octet-stream"

	contentTypeHeader = "Content-Type"
)

var ErrPartOutsideParent = errors.New("file outside parent dir")
var ErrPartInChildTree = errors.New("file in child tree")

// multipartFile implements Node, and is created from a `multipart.Part`.
type multipartFile struct {
	Node

	part      *multipart.Part
	reader    *peekReader
	mediatype string
}

func NewFileFromPartReader(reader *multipart.Reader, mediatype string) (Directory, error) {
	if !isDirectory(mediatype) {
		return nil, ErrNotDirectory
	}

	f := &multipartFile{
		reader:    &peekReader{r: reader},
		mediatype: mediatype,
	}

	return f, nil
}

func newFileFromPart(parent string, part *multipart.Part, reader *peekReader) (string, Node, error) {
	f := &multipartFile{
		part:   part,
		reader: reader,
	}

	dir, base := path.Split(f.fileName())
	dir = path.Clean(dir)
	parent = path.Clean(parent)
	if dir == "." {
		dir = ""
	}
	if parent == "." {
		parent = ""
	}

	if dir != parent {
		if strings.HasPrefix(dir, parent) {
			return "", nil, ErrPartInChildTree
		}
		return "", nil, ErrPartOutsideParent
	}

	contentType := part.Header.Get(contentTypeHeader)
	switch contentType {
	case applicationSymlink:
		out, err := ioutil.ReadAll(part)
		if err != nil {
			return "", nil, err
		}

		return base, NewLinkFile(string(out), nil), nil
	case "": // default to application/octet-stream
		fallthrough
	case applicationFile:
		return base, &ReaderFile{
			reader:  part,
			abspath: part.Header.Get("abspath"),
		}, nil
	}

	var err error
	f.mediatype, _, err = mime.ParseMediaType(contentType)
	if err != nil {
		return "", nil, err
	}

	if !isDirectory(f.mediatype) {
		return base, &ReaderFile{
			reader:  part,
			abspath: part.Header.Get("abspath"),
		}, nil
	}

	return base, f, nil
}

func isDirectory(mediatype string) bool {
	return mediatype == multipartFormdataType || mediatype == applicationDirectory
}

type multipartIterator struct {
	f *multipartFile

	curFile Node
	curName string
	err     error
}

func (it *multipartIterator) Name() string {
	return it.curName
}

func (it *multipartIterator) Node() Node {
	return it.curFile
}

func (it *multipartIterator) Next() bool {
	if it.f.reader == nil {
		return false
	}
	var part *multipart.Part
	for {
		var err error
		part, err = it.f.reader.NextPart()
		if err != nil {
			if err == io.EOF {
				return false
			}
			it.err = err
			return false
		}

		name, cf, err := newFileFromPart(it.f.fileName(), part, it.f.reader)
		if err == ErrPartOutsideParent {
			break
		}
		if err != ErrPartInChildTree {
			it.curFile = cf
			it.curName = name
			it.err = err
			return err == nil
		}
	}

	it.err = it.f.reader.put(part)
	return false
}

func (it *multipartIterator) Err() error {
	return it.err
}

func (f *multipartFile) Entries() DirIterator {
	return &multipartIterator{f: f}
}

func (f *multipartFile) fileName() string {
	if f == nil || f.part == nil {
		return ""
	}

	filename, err := url.QueryUnescape(f.part.FileName())
	if err != nil {
		// if there is a unescape error, just treat the name as unescaped
		return f.part.FileName()
	}
	return filename
}

func (f *multipartFile) Close() error {
	if f.part != nil {
		return f.part.Close()
	}
	return nil
}

func (f *multipartFile) Size() (int64, error) {
	return 0, ErrNotSupported
}

type PartReader interface {
	NextPart() (*multipart.Part, error)
}

type peekReader struct {
	r    PartReader
	next *multipart.Part
}

func (pr *peekReader) NextPart() (*multipart.Part, error) {
	if pr.next != nil {
		p := pr.next
		pr.next = nil
		return p, nil
	}

	if pr.r == nil {
		return nil, io.EOF
	}

	p, err := pr.r.NextPart()
	if err == io.EOF {
		pr.r = nil
	}
	return p, err
}

func (pr *peekReader) put(p *multipart.Part) error {
	if pr.next != nil {
		return errors.New("cannot put multiple parts")
	}
	pr.next = p
	return nil
}

var _ Directory = &multipartFile{}
