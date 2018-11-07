package files

import (
	"errors"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/url"
	"path"
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

// MultipartFile implements File, and is created from a `multipart.Part`.
// It can be either a directory or file (checked by calling `IsDirectory()`).
type MultipartFile struct {
	File

	Part      *multipart.Part
	Reader    PartReader
	Mediatype string
}

func NewFileFromPartReader(reader *multipart.Reader, mediatype string) (File, error) {
	if !isDirectory(mediatype) {
		return nil, ErrNotDirectory
	}

	f := &MultipartFile{
		Reader:    &peekReader{r: reader},
		Mediatype: mediatype,
	}

	return f, nil
}

func newFileFromPart(parent string, part *multipart.Part, reader PartReader) (string, File, error) {
	f := &MultipartFile{
		Part:   part,
		Reader: reader,
	}

	dir, base := path.Split(f.fileName())
	if path.Clean(dir) != path.Clean(parent) {
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
	f.Mediatype, _, err = mime.ParseMediaType(contentType)
	if err != nil {
		return "", nil, err
	}

	if !isDirectory(f.Mediatype) {
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
	f *MultipartFile

	curFile File
	curName string
	err     error
}

func (it *multipartIterator) Name() string {
	return it.curName
}

func (it *multipartIterator) File() File {
	return it.curFile
}

func (it *multipartIterator) Regular() Regular {
	return castRegular(it.File())
}

func (it *multipartIterator) Dir() Directory {
	return castDir(it.File())
}

func (it *multipartIterator) Next() bool {
	if it.f.Reader == nil {
		return false
	}
	part, err := it.f.Reader.NextPart()
	if err != nil {
		it.err = err
		return false
	}

	name, cf, err := newFileFromPart(it.f.fileName(), part, it.f.Reader)
	if err != ErrPartOutsideParent {
		it.curFile = cf
		it.curName = name
		it.err = err
		return err == nil
	}

	// we read too much, try to fix this
	pr, ok := it.f.Reader.(*peekReader)
	if !ok {
		it.err = errors.New("cannot undo NextPart")
		return false
	}

	it.err = pr.put(part)
	return false
}

func (it *multipartIterator) Err() error {
	panic("implement me")
}

func (f *MultipartFile) Entries() (DirIterator, error) {
	return &multipartIterator{f: f}, nil
}

func (f *MultipartFile) fileName() string {
	if f == nil || f.Part == nil {
		return ""
	}

	filename, err := url.QueryUnescape(f.Part.FileName())
	if err != nil {
		// if there is a unescape error, just treat the name as unescaped
		return f.Part.FileName()
	}
	return filename
}

func (f *MultipartFile) Close() error {
	if f.Part != nil {
		return f.Part.Close()
	}
	return nil
}

func (f *MultipartFile) Size() (int64, error) {
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

	return pr.r.NextPart()
}

func (pr *peekReader) put(p *multipart.Part) error {
	if pr.next != nil {
		return errors.New("cannot put multiple parts")
	}
	pr.next = p
	return nil
}

var _ Directory = &MultipartFile{}
