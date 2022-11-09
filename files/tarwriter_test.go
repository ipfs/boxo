package files

import (
	"archive/tar"
	"errors"
	"io"
	"testing"
	"time"
)

func TestTarWriter(t *testing.T) {
	tf := NewMapDirectory(map[string]Node{
		"file.txt": NewBytesFile([]byte(text)),
		"boop": NewMapDirectory(map[string]Node{
			"a.txt": NewBytesFile([]byte("bleep")),
			"b.txt": NewBytesFile([]byte("bloop")),
		}),
		"beep.txt": NewBytesFile([]byte("beep")),
	})

	pr, pw := io.Pipe()
	tw, err := NewTarWriter(pw)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(pr)

	go func() {
		defer tw.Close()
		if err := tw.WriteFile(tf, ""); err != nil {
			t.Error(err)
		}
	}()

	var cur *tar.Header

	checkHeader := func(name string, typ byte, size int64) {
		if cur.Name != name {
			t.Errorf("got wrong name: %s != %s", cur.Name, name)
		}
		if cur.Typeflag != typ {
			t.Errorf("got wrong type: %d != %d", cur.Typeflag, typ)
		}
		if cur.Size != size {
			t.Errorf("got wrong size: %d != %d", cur.Size, size)
		}
		now := time.Now()
		if cur.ModTime.After(now) {
			t.Errorf("wrote timestamp in the future: %s (now) < %s", now, cur.ModTime)
		}
	}

	if cur, err = tr.Next(); err != nil {
		t.Fatal(err)
	}
	checkHeader("", tar.TypeDir, 0)

	if cur, err = tr.Next(); err != nil {
		t.Fatal(err)
	}
	checkHeader("beep.txt", tar.TypeReg, 4)

	if cur, err = tr.Next(); err != nil {
		t.Fatal(err)
	}
	checkHeader("boop", tar.TypeDir, 0)

	if cur, err = tr.Next(); err != nil {
		t.Fatal(err)
	}
	checkHeader("boop/a.txt", tar.TypeReg, 5)

	if cur, err = tr.Next(); err != nil {
		t.Fatal(err)
	}
	checkHeader("boop/b.txt", tar.TypeReg, 5)

	if cur, err = tr.Next(); err != nil {
		t.Fatal(err)
	}
	checkHeader("file.txt", tar.TypeReg, 13)

	if cur, err = tr.Next(); err != io.EOF {
		t.Fatal(err)
	}
}

func TestTarWriterRelativePathInsideRoot(t *testing.T) {
	tf := NewMapDirectory(map[string]Node{
		"file.txt": NewBytesFile([]byte(text)),
		"boop": NewMapDirectory(map[string]Node{
			"../a.txt": NewBytesFile([]byte("bleep")),
			"b.txt":    NewBytesFile([]byte("bloop")),
		}),
		"beep.txt": NewBytesFile([]byte("beep")),
	})

	tw, err := NewTarWriter(io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	defer tw.Close()
	if err := tw.WriteFile(tf, ""); err != nil {
		t.Error(err)
	}
}

func TestTarWriterFailsFileOutsideRoot(t *testing.T) {
	tf := NewMapDirectory(map[string]Node{
		"file.txt": NewBytesFile([]byte(text)),
		"boop": NewMapDirectory(map[string]Node{
			"../../a.txt": NewBytesFile([]byte("bleep")),
			"b.txt":       NewBytesFile([]byte("bloop")),
		}),
		"beep.txt": NewBytesFile([]byte("beep")),
	})

	tw, err := NewTarWriter(io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	defer tw.Close()
	if err := tw.WriteFile(tf, ""); !errors.Is(err, ErrUnixFSPathOutsideRoot) {
		t.Errorf("unexpected error, wanted: %v; got: %v", ErrUnixFSPathOutsideRoot, err)
	}
}

func TestTarWriterFailsFileOutsideRootWithBaseDir(t *testing.T) {
	tf := NewMapDirectory(map[string]Node{
		"../file.txt": NewBytesFile([]byte(text)),
		"boop": NewMapDirectory(map[string]Node{
			"a.txt": NewBytesFile([]byte("bleep")),
			"b.txt": NewBytesFile([]byte("bloop")),
		}),
		"beep.txt": NewBytesFile([]byte("beep")),
	})

	tw, err := NewTarWriter(io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	defer tw.Close()
	if err := tw.WriteFile(tf, "test.tar"); !errors.Is(err, ErrUnixFSPathOutsideRoot) {
		t.Errorf("unexpected error, wanted: %v; got: %v", ErrUnixFSPathOutsideRoot, err)
	}
}
