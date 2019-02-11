package files

import (
	"io"
	"mime/multipart"
	"strings"
	"testing"
)

func TestSliceFiles(t *testing.T) {
	sf := NewMapDirectory(map[string]Node{
		"1": NewBytesFile([]byte("Some text!\n")),
		"2": NewBytesFile([]byte("beep")),
		"3": NewBytesFile([]byte("boop")),
	})

	CheckDir(t, sf, []Event{
		{
			kind:  TFile,
			name:  "1",
			value: "Some text!\n",
		},
		{
			kind:  TFile,
			name:  "2",
			value: "beep",
		},
		{
			kind:  TFile,
			name:  "3",
			value: "boop",
		},
	})
}

func TestReaderFiles(t *testing.T) {
	message := "beep boop"
	rf := NewBytesFile([]byte(message))
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
--Boundary!
Content-Type: text/plain
Content-Disposition: file; filename="implicit1/implicit2/deep_implicit"

implicit file1
--Boundary!
Content-Type: text/plain
Content-Disposition: file; filename="implicit1/shallow_implicit"

implicit file2
--Boundary!--

`

	reader := strings.NewReader(data)
	mpReader := multipart.NewReader(reader, "Boundary!")
	dir, err := NewFileFromPartReader(mpReader, multipartFormdataType)
	if err != nil {
		t.Fatal(err)
	}

	CheckDir(t, dir, []Event{
		{
			kind:  TFile,
			name:  "name",
			value: "beep",
		},
		{
			kind: TDirStart,
			name: "dir",
		},
		{
			kind:  TFile,
			name:  "nested",
			value: "some content",
		},
		{
			kind:  TSymlink,
			name:  "simlynk",
			value: "anotherfile",
		},
		{
			kind: TDirEnd,
		},
		{
			kind: TDirStart,
			name: "implicit1",
		},
		{
			kind: TDirStart,
			name: "implicit2",
		},
		{
			kind:  TFile,
			name:  "deep_implicit",
			value: "implicit file1",
		},
		{
			kind: TDirEnd,
		},
		{
			kind:  TFile,
			name:  "shallow_implicit",
			value: "implicit file2",
		},
		{
			kind: TDirEnd,
		},
	})
}
