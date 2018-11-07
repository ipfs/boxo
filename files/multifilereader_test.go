package files

import (
	"io"
	"io/ioutil"
	"mime/multipart"
	"strings"
	"testing"
)

var text = "Some text! :)"

func getTestMultiFileReader() *MultiFileReader {
	fileset := []FileEntry{
		{NewReaderFile(ioutil.NopCloser(strings.NewReader(text)), nil), "file.txt"},
		{NewSliceFile([]FileEntry{
			{NewReaderFile(ioutil.NopCloser(strings.NewReader("bleep")), nil), "a.txt"},
			{NewReaderFile(ioutil.NopCloser(strings.NewReader("bloop")), nil), "b.txt"},
		}), "boop"},
		{NewReaderFile(ioutil.NopCloser(strings.NewReader("beep")), nil), "beep.txt"},
	}
	sf := NewSliceFile(fileset)

	// testing output by reading it with the go stdlib "mime/multipart" Reader
	return NewMultiFileReader(sf, true)
}

func TestMultiFileReaderToMultiFile(t *testing.T) {
	mfr := getTestMultiFileReader()
	mpReader := multipart.NewReader(mfr, mfr.Boundary())
	mf, err := NewFileFromPartReader(mpReader, multipartFormdataType)
	if err != nil {
		t.Fatal(err)
	}

	md, ok := mf.(Directory)
	if !ok {
		t.Fatal("Expected a directory")
	}

	fn, f, err := md.NextFile()
	if fn != "file.txt" || f == nil || err != nil {
		t.Fatal("NextFile returned unexpected data")
	}

	dn, d, err := md.NextFile()
	if dn != "boop" || d == nil || err != nil {
		t.Fatal("NextFile returned unexpected data")
	}

	df, ok := d.(Directory)
	if !ok {
		t.Fatal("Expected a directory")
	}

	cfn, cf, err := df.NextFile()
	if cfn != "a.txt" || cf == nil || err != nil {
		t.Fatal("NextFile returned unexpected data")
	}

	cfn, cf, err = df.NextFile()
	if cfn != "b.txt" || cf == nil || err != nil {
		t.Fatal("NextFile returned unexpected data")
	}

	cfn, cf, err = df.NextFile()
	if cfn != "" || cf != nil || err != io.EOF {
		t.Fatal("NextFile returned unexpected data")
	}

	// try to break internal state
	cfn, cf, err = df.NextFile()
	if cfn != "" || cf != nil || err != io.EOF {
		t.Fatal("NextFile returned unexpected data")
	}

	fn, f, err = md.NextFile()
	if fn != "beep.txt" || f == nil || err != nil {
		t.Fatal("NextFile returned unexpected data")
	}
}

func TestOutput(t *testing.T) {
	mfr := getTestMultiFileReader()
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

	cname, child, err = mpd.NextFile()
	if child != nil || err != io.EOF {
		t.Fatal("Expected to get (nil, io.EOF)")
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
