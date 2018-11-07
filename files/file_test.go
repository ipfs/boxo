package files

import (
	"io"
	"io/ioutil"
	"mime/multipart"
	"strings"
	"testing"
)

func TestSliceFiles(t *testing.T) {
	files := []FileEntry{
		{NewReaderFile(ioutil.NopCloser(strings.NewReader("Some text!\n")), nil), ""},
		{NewReaderFile(ioutil.NopCloser(strings.NewReader("beep")), nil), ""},
		{NewReaderFile(ioutil.NopCloser(strings.NewReader("boop")), nil), ""},
	}
	buf := make([]byte, 20)

	sf := NewSliceFile(files)

	_, file, err := sf.NextFile()
	if file == nil || err != nil {
		t.Fatal("Expected a file and nil error")
	}
	rf, ok := file.(Regular)
	if !ok {
		t.Fatal("Expected a regular file")
	}
	read, err := rf.Read(buf)
	if read != 11 || err != nil {
		t.Fatal("NextFile got a file in the wrong order")
	}

	_, file, err = sf.NextFile()
	if file == nil || err != nil {
		t.Fatal("Expected a file and nil error")
	}
	_, file, err = sf.NextFile()
	if file == nil || err != nil {
		t.Fatal("Expected a file and nil error")
	}

	_, file, err = sf.NextFile()
	if file != nil || err != io.EOF {
		t.Fatal("Expected a nil file and io.EOF")
	}

	if err := sf.Close(); err != nil {
		t.Fatal("Should be able to call `Close` on a SliceFile")
	}
}

func TestReaderFiles(t *testing.T) {
	message := "beep boop"
	rf := NewReaderFile(ioutil.NopCloser(strings.NewReader(message)), nil)
	buf := make([]byte, len(message))

	if n, err := rf.Read(buf); n == 0 || err != nil {
		t.Fatal("Expected to be able to read")
	}
	if err := rf.Close(); err != nil {
		t.Fatal("Should be able to close")
	}
	if n, err := rf.Read(buf); n != 0 || err != io.EOF {
		t.Fatal("Expected EOF when reading after close")
	}
}

func TestMultipartFiles(t *testing.T) {
	data := `
--Boundary!
Content-Type: text/plain
Content-Disposition: file; filename="name"
Some-Header: beep

beep
--Boundary!
Content-Type: application/x-directory
Content-Disposition: file; filename="dir"

--Boundary!
Content-Type: text/plain
Content-Disposition: file; filename="dir/nested"

some content
--Boundary!
Content-Type: application/symlink
Content-Disposition: file; filename="dir/simlynk"

anotherfile
--Boundary!--

`

	reader := strings.NewReader(data)
	mpReader := multipart.NewReader(reader, "Boundary!")
	buf := make([]byte, 20)

	// test properties of a file created from the first part
	part, err := mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpname, mpf, err := newFileFromPart("", part, mpReader)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil MultipartFile, nil error")
	}
	mf, ok := mpf.(Regular)
	if !ok {
		t.Fatal("Expected file to not be a directory")
	}
	if mpname != "name" {
		t.Fatal("Expected filename to be \"name\"")
	}
	if n, err := mf.Read(buf); n != 4 || !(err == io.EOF || err == nil) {
		t.Fatal("Expected to be able to read 4 bytes", n, err)
	}
	if err := mf.Close(); err != nil {
		t.Fatal("Expected to be able to close file")
	}

	// test properties of file created from second part (directory)
	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpname, mpf, err = newFileFromPart("", part, mpReader)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil MultipartFile, nil error")
	}
	md, ok := mpf.(Directory)
	if !ok {
		t.Fatal("Expected file to be a directory")
	}
	if mpname != "dir" {
		t.Fatal("Expected filename to be \"dir\"")
	}
	if err := md.Close(); err != nil {
		t.Fatal("Should be able to call `Close` on a directory")
	}

	// test properties of file created from third part (nested file)
	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpname, mpf, err = newFileFromPart("dir/", part, mpReader)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil MultipartFile, nil error")
	}
	mf, ok = mpf.(Regular)
	if !ok {
		t.Fatal("Expected file to not be a directory")
	}
	if mpname != "nested" {
		t.Fatalf("Expected filename to be \"nested\", got %s", mpname)
	}
	if n, err := mf.Read(buf); n != 12 || !(err == nil || err == io.EOF) {
		t.Fatalf("expected to be able to read 12 bytes from file: %s (got %d)", err, n)
	}
	if err := mpf.Close(); err != nil {
		t.Fatalf("should be able to close file: %s", err)
	}

	// test properties of symlink created from fourth part (symlink)
	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpname, mpf, err = newFileFromPart("dir/", part, mpReader)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil MultipartFile, nil error")
	}
	ms, ok := mpf.(*Symlink)
	if !ok {
		t.Fatal("Expected file to not be a directory")
	}
	if mpname != "simlynk" {
		t.Fatal("Expected filename to be \"dir/simlynk\"")
	}
	if ms.Target != "anotherfile" {
		t.Fatal("expected link to point to anotherfile")
	}
}
