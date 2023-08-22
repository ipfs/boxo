package files

import (
	"bytes"
	"io"
	"mime/multipart"
	"testing"

	"github.com/stretchr/testify/require"
)

var text = "Some text! :)"

func newBytesFileWithPath(abspath string, b []byte) File {
	return &ReaderFile{abspath, bytesReaderCloser{bytes.NewReader(b)}, nil, int64(len(b))}
}

func makeMultiFileReader(t *testing.T, binaryFileName, rawAbsPath bool) (string, *MultiFileReader) {
	var (
		filename string
		file     File
	)

	if binaryFileName {
		filename = "bad\x7fname.txt"
		file = newBytesFileWithPath("/my/path/boop/bad\x7fname.txt", []byte("bloop"))
	} else {
		filename = "résumé🥳.txt"
		file = newBytesFileWithPath("/my/path/boop/résumé🥳.txt", []byte("bloop"))
	}

	sf := NewMapDirectory(map[string]Node{
		"file.txt": newBytesFileWithPath("/my/path/file.txt", []byte(text)),
		"boop": NewMapDirectory(map[string]Node{
			"a.txt":  newBytesFileWithPath("/my/path/boop/a.txt", []byte("bleep")),
			filename: file,
		}),
		"beep.txt": newBytesFileWithPath("/my/path/beep.txt", []byte("beep")),
	})

	return filename, NewMultiFileReader(sf, true, rawAbsPath)
}

func runMultiFileReaderToMultiFileTest(t *testing.T, binaryFileName, rawAbsPath, expectFailure bool) {
	filename, mfr := makeMultiFileReader(t, binaryFileName, rawAbsPath)
	mpReader := multipart.NewReader(mfr, mfr.Boundary())
	mf, err := NewFileFromPartReader(mpReader, multipartFormdataType)
	if err != nil {
		t.Fatal(err)
	}

	it := mf.Entries()

	require.True(t, it.Next())
	require.Equal(t, "beep.txt", it.Name())
	require.True(t, it.Next())
	require.Equal(t, "boop", it.Name())
	require.NotNil(t, DirFromEntry(it))

	subIt := DirFromEntry(it).Entries()
	require.True(t, subIt.Next(), subIt.Err())
	require.Equal(t, "a.txt", subIt.Name())
	require.Nil(t, DirFromEntry(subIt))

	if expectFailure {
		require.False(t, subIt.Next())
		require.Error(t, subIt.Err())
	} else {
		require.True(t, subIt.Next(), subIt.Err())
		require.Equal(t, filename, subIt.Name())
		require.Nil(t, DirFromEntry(subIt))

		require.False(t, subIt.Next())
		require.Nil(t, it.Err())

		// try to break internal state
		require.False(t, subIt.Next())
		require.Nil(t, it.Err())

		require.True(t, it.Next())
		require.Equal(t, "file.txt", it.Name())
		require.Nil(t, DirFromEntry(it))
		require.Nil(t, it.Err())

		require.False(t, it.Next())
		require.Nil(t, it.Err())
	}
}

func TestMultiFileReaderToMultiFile(t *testing.T) {
	t.Run("Header 'abspath' with unicode filename succeeds", func(t *testing.T) {
		runMultiFileReaderToMultiFileTest(t, false, true, false)
	})

	t.Run("Header 'abspath-encoded' with unicode filename succeeds", func(t *testing.T) {
		runMultiFileReaderToMultiFileTest(t, false, false, false)
	})

	t.Run("Header 'abspath-encoded' with binary filename succeeds", func(t *testing.T) {
		runMultiFileReaderToMultiFileTest(t, true, false, false)
	})
}

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
	return NewMultiFileReader(sf, true, false)
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
	mfr := NewMultiFileReader(sf, true, false)
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
