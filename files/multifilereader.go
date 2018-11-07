package files

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"net/url"
	"path"
	"sync"
)

// MultiFileReader reads from a `commands.File` (which can be a directory of files
// or a regular file) as HTTP multipart encoded data.
type MultiFileReader struct {
	io.Reader

	// directory stack for NextFile
	files []Directory
	path  []string

	currentFile File
	buf         bytes.Buffer
	mpWriter    *multipart.Writer
	closed      bool
	mutex       *sync.Mutex

	// if true, the data will be type 'multipart/form-data'
	// if false, the data will be type 'multipart/mixed'
	form bool
}

// NewMultiFileReader constructs a MultiFileReader. `file` can be any `commands.Directory`.
// If `form` is set to true, the multipart data will have a Content-Type of 'multipart/form-data',
// if `form` is false, the Content-Type will be 'multipart/mixed'.
func NewMultiFileReader(file Directory, form bool) *MultiFileReader {
	mfr := &MultiFileReader{
		files: []Directory{file},
		path:  []string{""},
		form:  form,
		mutex: &sync.Mutex{},
	}
	mfr.mpWriter = multipart.NewWriter(&mfr.buf)

	return mfr
}

func (mfr *MultiFileReader) Read(buf []byte) (written int, err error) {
	mfr.mutex.Lock()
	defer mfr.mutex.Unlock()

	// if we are closed and the buffer is flushed, end reading
	if mfr.closed && mfr.buf.Len() == 0 {
		return 0, io.EOF
	}

	// if the current file isn't set, advance to the next file
	if mfr.currentFile == nil {
		var file File
		var name string

		for file == nil {
			if len(mfr.files) == 0 {
				mfr.mpWriter.Close()
				mfr.closed = true
				return mfr.buf.Read(buf)
			}

			nextName, nextFile, err := mfr.files[len(mfr.files)-1].NextFile()
			if err == io.EOF {
				mfr.files = mfr.files[:len(mfr.files)-1]
				mfr.path = mfr.path[:len(mfr.path)-1]
				continue
			} else if err != nil {
				return 0, err
			}

			file = nextFile
			name = nextName
		}

		// handle starting a new file part
		if !mfr.closed {

			mfr.currentFile = file

			// write the boundary and headers
			header := make(textproto.MIMEHeader)
			filename := url.QueryEscape(path.Join(path.Join(mfr.path...), name))
			header.Set("Content-Disposition", fmt.Sprintf("file; filename=\"%s\"", filename))

			var contentType string

			switch f := file.(type) {
			case *Symlink:
				contentType = "application/symlink"
			case Directory:
				mfr.files = append(mfr.files, f)
				mfr.path = append(mfr.path, name)
				contentType = "application/x-directory"
			case Regular:
				// otherwise, use the file as a reader to read its contents
				contentType = "application/octet-stream"
			default:
				return 0, ErrNotSupported
			}

			header.Set("Content-Type", contentType)
			if rf, ok := file.(FileInfo); ok {
				header.Set("abspath", rf.AbsPath())
			}

			_, err := mfr.mpWriter.CreatePart(header)
			if err != nil {
				return 0, err
			}
		}
	}

	// if the buffer has something in it, read from it
	if mfr.buf.Len() > 0 {
		return mfr.buf.Read(buf)
	}

	// otherwise, read from file data
	switch f := mfr.currentFile.(type) {
	case Regular:
		written, err = f.Read(buf)
		if err != io.EOF {
			return written, err
		}
	}

	if err := mfr.currentFile.Close(); err != nil {
		return written, err
	}

	mfr.currentFile = nil
	return written, nil
}

// Boundary returns the boundary string to be used to separate files in the multipart data
func (mfr *MultiFileReader) Boundary() string {
	return mfr.mpWriter.Boundary()
}
