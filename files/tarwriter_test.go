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
		"boop": NewMapStatDirectory(map[string]Node{
			"a.txt": NewBytesFile([]byte("bleep")),
			"b.txt": NewBytesFile([]byte("bloop")),
		}, &mockFileInfo{name: "", mode: 0o750, mtime: time.Unix(6600000000, 0)}),
		"beep.txt": NewBytesStatFile([]byte("beep"),
			&mockFileInfo{name: "beep.txt", size: 4, mode: 0o766, mtime: time.Unix(1604320500, 54321)}),
		"boop-sl": NewSymlinkFile("boop", time.Unix(6600050000, 0)),
	})

	pr, pw := io.Pipe()
	tw, err := NewTarWriter(pw)
	if err != nil {
		t.Fatal(err)
	}
	tw.SetFormat(tar.FormatPAX)
	tr := tar.NewReader(pr)

	go func() {
		defer tw.Close()
		if err := tw.WriteFile(tf, ""); err != nil {
			t.Error(err)
		}
	}()

	var cur *tar.Header
	const delta = 4 * time.Second

	checkHeader := func(name string, typ byte, size int64, mode int64, mtime time.Time) {
		if cur.Name != name {
			t.Errorf("got wrong name: %s != %s", cur.Name, name)
		}
		if cur.Typeflag != typ {
			t.Errorf("got wrong type: %d != %d", cur.Typeflag, typ)
		}
		if cur.Size != size {
			t.Errorf("got wrong size: %d != %d", cur.Size, size)
		}
		if cur.Mode != mode {
			t.Errorf("got wrong mode: %d != %d", cur.Mode, mode)
		}
		if mtime.IsZero() {
			interval := time.Since(cur.ModTime)
			if interval < -delta || interval > delta {
				t.Errorf("expected timestamp to be current: %s", cur.ModTime)
			}
		} else if cur.ModTime.UnixNano() != mtime.UnixNano() {
			t.Errorf("got wrong timestamp: %s != %s", cur.ModTime, mtime)
		}
	}

	if cur, err = tr.Next(); err != nil {
		t.Fatal(err)
	}
	checkHeader("", tar.TypeDir, 0, 0o755, time.Time{})

	if cur, err = tr.Next(); err != nil {
		t.Fatal(err)
	}
	checkHeader("beep.txt", tar.TypeReg, 4, 0o766, time.Unix(1604320500, 54321))

	if cur, err = tr.Next(); err != nil {
		t.Fatal(err)
	}
	checkHeader("boop", tar.TypeDir, 0, 0o750, time.Unix(6600000000, 0))

	if cur, err = tr.Next(); err != nil {
		t.Fatal(err)
	}
	checkHeader("boop/a.txt", tar.TypeReg, 5, 0o644, time.Time{})

	if cur, err = tr.Next(); err != nil {
		t.Fatal(err)
	}
	checkHeader("boop/b.txt", tar.TypeReg, 5, 0o644, time.Time{})

	if cur, err = tr.Next(); err != nil {
		t.Fatal(err)
	}
	checkHeader("boop-sl", tar.TypeSymlink, 0, 0o777, time.Unix(6600050000, 0))
	if cur, err = tr.Next(); err != nil {
		t.Fatal(err)
	}
	checkHeader("file.txt", tar.TypeReg, 13, 0o644, time.Time{})

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
	if err = tw.WriteFile(tf, ""); err != nil {
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
	if err = tw.WriteFile(tf, ""); !errors.Is(err, ErrUnixFSPathOutsideRoot) {
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
	if err = tw.WriteFile(tf, "test.tar"); !errors.Is(err, ErrUnixFSPathOutsideRoot) {
		t.Errorf("unexpected error, wanted: %v; got: %v", ErrUnixFSPathOutsideRoot, err)
	}
}
