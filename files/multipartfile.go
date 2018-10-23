package files

import (
	"errors"
	"io"
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
	f := &MultipartFile{
		Reader: &peekReader{r: reader},
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

		return base, &Symlink{
			Target: string(out),
		}, nil
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

	return base, f, nil
}

func (f *MultipartFile) IsDirectory() bool {
	return f.Mediatype == multipartFormdataType || f.Mediatype == applicationDirectory
}

func (f *MultipartFile) NextFile() (string, File, error) {
	if !f.IsDirectory() {
		return "", nil, ErrNotDirectory
	}
	if f.Reader == nil {
		return "", nil, io.EOF
	}
	part, err := f.Reader.NextPart()
	if err != nil {
		return "", nil, err
	}

	name, cf, err := newFileFromPart(f.fileName(), part, f.Reader)
	if err != ErrPartOutsideParent {
		return name, cf, err
	}

	// we read too much, try to fix this
	pr, ok := f.Reader.(*peekReader)
	if !ok {
		return "", nil, errors.New("cannot undo NextPart")
	}

	if err := pr.put(part); err != nil {
		return "", nil, err
	}
	return "", nil, io.EOF
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

func (f *MultipartFile) Read(p []byte) (int, error) {
	if f.IsDirectory() {
		return 0, ErrNotReader
	}
	return f.Part.Read(p)
}

func (f *MultipartFile) Close() error {
	if f.IsDirectory() {
		return ErrNotReader
	}
	return f.Part.Close()
}

func (f *MultipartFile) Seek(offset int64, whence int) (int64, error) {
	if f.IsDirectory() {
		return 0, ErrNotReader
	}
	return 0, ErrNotReader
}

func (f *MultipartFile) Size() (int64, error) {
	return 0, ErrNotReader
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
