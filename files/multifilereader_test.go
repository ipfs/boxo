package files

import (
	"io"
	"mime/multipart"
	"testing"
)

var text = "Some text! :)"

func getTestMultiFileReader(t *testing.T) *MultiFileReader {
	sf := NewMapDirectory(map[string]Node{
		"file.txt": NewBytesFile([]byte(text)),
		"boop": NewMapDirectory(map[string]Node{
			"a.txt": NewBytesFile([]byte("bleep")),
			"b.txt": NewBytesFile([]byte("bloop")),
		}),
		"beep.txt": NewBytesFile([]byte("beep")),
	})

	// testing output by reading it with the go stdlib "mime/multipart" Reader
	return NewMultiFileReader(sf, true)
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
	it := md.Entries()

	if !it.Next() || it.Name() != "beep.txt" {
		t.Fatal("iterator didn't work as expected")
	}

	if !it.Next() || it.Name() != "boop" || DirFromEntry(it) == nil {
		t.Fatal("iterator didn't work as expected")
	}

	subIt := DirFromEntry(it).Entries()

	if !subIt.Next() || subIt.Name() != "a.txt" || DirFromEntry(subIt) != nil {
		t.Fatal("iterator didn't work as expected")
	}

	if !subIt.Next() || subIt.Name() != "b.txt" || DirFromEntry(subIt) != nil {
		t.Fatal("iterator didn't work as expected")
	}

	if subIt.Next() || it.Err() != nil {
		t.Fatal("iterator didn't work as expected")
	}

	// try to break internal state
	if subIt.Next() || it.Err() != nil {
		t.Fatal("iterator didn't work as expected")
	}

	if !it.Next() || it.Name() != "file.txt" || DirFromEntry(it) != nil || it.Err() != nil {
		t.Fatal("iterator didn't work as expected")
	}

	if it.Next() || it.Err() != nil {
		t.Fatal("iterator didn't work as expected")
	}
}

func TestMultiFileReaderToMultiFileSkip(t *testing.T) {
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
	it := md.Entries()

	if !it.Next() || it.Name() != "beep.txt" {
		t.Fatal("iterator didn't work as expected")
	}

	if !it.Next() || it.Name() != "boop" || DirFromEntry(it) == nil {
		t.Fatal("iterator didn't work as expected")
	}

	if !it.Next() || it.Name() != "file.txt" || DirFromEntry(it) != nil || it.Err() != nil {
		t.Fatal("iterator didn't work as expected")
	}

	if it.Next() || it.Err() != nil {
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
		t.Fatal("Expected non-nil multipartFile, nil error")
	}
	mpr, ok := mpf.(File)
	if !ok {
		t.Fatal("Expected file to be a regular file")
	}
	if mpname != "beep.txt" {
		t.Fatal("Expected filename to be \"file.txt\"")
	}
	if n, err := mpr.Read(buf); n != 4 || err != nil {
		t.Fatal("Expected to read from file", n, err)
	}
	if string(buf[:4]) != "beep" {
		t.Fatal("Data read was different than expected")
	}

	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpname, mpf, err = newFileFromPart("", part, mpReader)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil multipartFile, nil error")
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
	if _, ok := child.(File); !ok {
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
	if _, ok := child.(File); !ok {
		t.Fatal("Expected file to not be a directory")
	}
	if cname != "b.txt" {
		t.Fatal("Expected filename to be \"b.txt\"")
	}

	it := mpd.Entries()
	if it.Next() {
		t.Fatal("Expected to get false")
	}

	part, err = mpReader.NextPart()
	if part == nil || err != nil {
		t.Fatal("Expected non-nil part, nil error")
	}
	mpname, mpf, err = newFileFromPart("", part, mpReader)
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil multipartFile, nil error")
	}
	if mpname != "file.txt" {
		t.Fatal("Expected filename to be \"b.txt\"")
	}

	part, err = mpReader.NextPart()
	if part != nil || err != io.EOF {
		t.Fatal("Expected to get (nil, io.EOF)")
	}
}
