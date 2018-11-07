package files

import (
	"io"
	"io/ioutil"
	"mime/multipart"
	"strings"
	"testing"
)

var text = "Some text! :)"

func getTestMultiFileReader(t *testing.T) *MultiFileReader {
	fileset := []DirEntry{
		FileEntry("file.txt", NewReaderFile(ioutil.NopCloser(strings.NewReader(text)), nil)),
		FileEntry("boop", NewSliceFile([]DirEntry{
			FileEntry("a.txt", NewReaderFile(ioutil.NopCloser(strings.NewReader("bleep")), nil)),
			FileEntry("b.txt", NewReaderFile(ioutil.NopCloser(strings.NewReader("bloop")), nil)),
		})),
		FileEntry("beep.txt", NewReaderFile(ioutil.NopCloser(strings.NewReader("beep")), nil)),
	}
	sf := NewSliceFile(fileset)

	// testing output by reading it with the go stdlib "mime/multipart" Reader
	r, err := NewMultiFileReader(sf, true)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestMultiFileReaderToMultiFile(t *testing.T) {
	mfr := getTestMultiFileReader(t)
	mpReader := multipart.NewReader(mfr, mfr.Boundary())
	mf, err := NewFileFromPartReader(mpReader, multipartFormdataType)
	if err != nil {
		t.Fatal(err)
	}

	md, ok := mf.(Directory)
	if !ok {
		t.Fatal("Expected a directory")
	}
	it, err := md.Entries()
	if err != nil {
		t.Fatal(err)
	}

	if !it.Next() || it.Name() != "file.txt" {
		t.Fatal("iterator didn't work as expected")
	}

	if !it.Next() || it.Name() != "boop" || it.Dir() == nil {
		t.Fatal("iterator didn't work as expected")
	}

	subIt, err := it.Dir().Entries()
	if err != nil {
		t.Fatal(err)
	}

	if !subIt.Next() || subIt.Name() != "a.txt" || subIt.Dir() != nil {
		t.Fatal("iterator didn't work as expected")
	}

	if !subIt.Next() || subIt.Name() != "b.txt" || subIt.Dir() != nil {
		t.Fatal("iterator didn't work as expected")
	}

	if subIt.Next() {
		t.Fatal("iterator didn't work as expected")
	}

	// try to break internal state
	if subIt.Next() {
		t.Fatal("iterator didn't work as expected")
	}

	if !it.Next() || it.Name() != "beep.txt" || it.Dir() != nil {
		t.Fatal("iterator didn't work as expected")
	}
}

func TestOutput(t *testing.T) {
	mfr := getTestMultiFileReader(t)
	mpReader := &peekReader{r: multipart.NewReader(mfr, mfr.Boundary())}
	buf := make([]byte, 20)

	part, err := mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpname, mpf, err := newFileFromPart("", part, mpReader)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil MultipartFile, nil error")
	}
	mpr, ok := mpf.(Regular)
	if !ok {
		t.Fatal("Expected file to be a regular file")
	}
	if mpname != "file.txt" {
		t.Fatal("Expected filename to be \"file.txt\"")
	}
	if n, err := mpr.Read(buf); n != len(text) || err != nil {
		t.Fatal("Expected to read from file", n, err)
	}
	if string(buf[:len(text)]) != text {
		t.Fatal("Data read was different than expected")
	}

	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpname, mpf, err = newFileFromPart("", part, mpReader)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil MultipartFile, nil error")
	}
	mpd, ok := mpf.(Directory)
	if !ok {
		t.Fatal("Expected file to be a directory")
	}
	if mpname != "boop" {
		t.Fatal("Expected filename to be \"boop\"")
	}

	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	cname, child, err := newFileFromPart("boop", part, mpReader)
	if child == nil || err != nil {
		t.Fatal("Expected to be able to read a child file")
	}
	if _, ok := child.(Regular); !ok {
		t.Fatal("Expected file to not be a directory")
	}
	if cname != "a.txt" {
		t.Fatal("Expected filename to be \"a.txt\"")
	}

	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	cname, child, err = newFileFromPart("boop", part, mpReader)
	if child == nil || err != nil {
		t.Fatal("Expected to be able to read a child file")
	}
	if _, ok := child.(Regular); !ok {
		t.Fatal("Expected file to not be a directory")
	}
	if cname != "b.txt" {
		t.Fatal("Expected filename to be \"b.txt\"")
	}

	it, err := mpd.Entries()
	if it.Next() {
		t.Fatal("Expected to get false")
	}

	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpname, mpf, err = newFileFromPart("", part, mpReader)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil MultipartFile, nil error")
	}
	if mpname != "beep.txt" {
		t.Fatal("Expected filename to be \"b.txt\"")
	}

	part, err = mpReader.NextPart()
	if part != nil || err != io.EOF {
		t.Fatal("Expected to get (nil, io.EOF)")
	}
}
