package files

import (
	"io"
	"mime/multipart"
	"strings"
	"testing"
	"time"
)

var text = "Some text! :)"

func getTestMultiFileReader(t *testing.T) *MultiFileReader {
	sf := NewMapDirectory(map[string]Node{
		"file.txt": NewReaderStatFile(
			strings.NewReader(text),
			&mockFileInfo{name: "file.txt", mode: 0, mtime: time.Time{}}),
		"boop": NewMapDirectory(map[string]Node{
			"a.txt": NewReaderStatFile(
				strings.NewReader("bleep"),
				&mockFileInfo{name: "a.txt", mode: 0744, mtime: time.Time{}}),
			"b.txt": NewReaderStatFile(
				strings.NewReader("bloop"),
				&mockFileInfo{name: "b.txt", mode: 0666, mtime: time.Unix(1604320500, 0)}),
		}),
		"beep.txt": NewReaderStatFile(
			strings.NewReader("beep"),
			&mockFileInfo{name: "beep.txt", mode: 0754, mtime: time.Unix(1604320500, 55555)}),
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

	it := mf.Entries()

	if !it.Next() || it.Name() != "beep.txt" {
		t.Fatal("iterator didn't work as expected")
	}

	n := it.Node()
	if n.Mode() != 0754 {
		t.Fatal("unexpected file mode")
	}
	if n.ModTime() != time.Unix(1604320500, 55555) {
		t.Fatal("unexpected last modification time")
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

	it := mf.Entries()

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
	walker := &multipartWalker{reader: multipart.NewReader(mfr, mfr.Boundary())}
	buf := make([]byte, 20)

	mpf, err := walker.nextFile()
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil multipartFile, nil error")
	}
	mpr, ok := mpf.(File)
	if !ok {
		t.Fatal("Expected file to be a regular file")
	}
	if n, err := mpr.Read(buf); n != 4 || err != nil {
		t.Fatal("Expected to read from file", n, err)
	}
	if string(buf[:4]) != "beep" {
		t.Fatal("Data read was different than expected")
	}

	mpf, err = walker.nextFile()
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil multipartFile, nil error")
	}
	mpd, ok := mpf.(Directory)
	if !ok {
		t.Fatal("Expected file to be a directory")
	}

	child, err := walker.nextFile()
	if child == nil || err != nil {
		t.Fatal("Expected to be able to read a child file")
	}
	if _, ok := child.(File); !ok {
		t.Fatal("Expected file to not be a directory")
	}

	child, err = walker.nextFile()
	if child == nil || err != nil {
		t.Fatal("Expected to be able to read a child file")
	}
	if _, ok := child.(File); !ok {
		t.Fatal("Expected file to not be a directory")
	}

	it := mpd.Entries()
	if it.Next() {
		t.Fatal("Expected to get false")
	}

	mpf, err = walker.nextFile()
	if mpf == nil || err != nil {
		t.Fatal("Expected non-nil multipartFile, nil error")
	}

	part, err := walker.getPart()
	if part != nil || err != io.EOF {
		t.Fatal("Expected to get (nil, io.EOF)")
	}
}

func TestCommonPrefix(t *testing.T) {
	sf := NewMapDirectory(map[string]Node{
		"boop": NewMapDirectory(map[string]Node{
			"a":   NewBytesFile([]byte("bleep")),
			"aa":  NewBytesFile([]byte("bleep")),
			"aaa": NewBytesFile([]byte("bleep")),
		}),
	})
	mfr := NewMultiFileReader(sf, true)
	reader, err := NewFileFromPartReader(multipart.NewReader(mfr, mfr.Boundary()), multipartFormdataType)
	if err != nil {
		t.Fatal(err)
	}

	CheckDir(t, reader, []Event{
		{
			kind: TDirStart,
			name: "boop",
		},
		{
			kind:  TFile,
			name:  "a",
			value: "bleep",
		},
		{
			kind:  TFile,
			name:  "aa",
			value: "bleep",
		},
		{
			kind:  TFile,
			name:  "aaa",
			value: "bleep",
		},
		{
			kind: TDirEnd,
		},
	})
}
