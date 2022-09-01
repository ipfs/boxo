package files

import (
	"io"
	"testing"
)

type Kind int

const (
	TFile Kind = iota
	TSymlink
	TDirStart
	TDirEnd
)

type Event struct {
	kind  Kind
	name  string
	value string
}

func CheckDir(t *testing.T, dir Directory, expected []Event) {
	expectedIndex := 0
	expect := func() (Event, int) {
		t.Helper()

		if expectedIndex > len(expected) {
			t.Fatal("no more expected entries")
		}
		i := expectedIndex
		expectedIndex++

		// Add an implicit "end" event at the end. It makes this
		// function a bit easier to write.
		next := Event{kind: TDirEnd}
		if i < len(expected) {
			next = expected[i]
		}
		return next, i
	}
	var check func(d Directory)
	check = func(d Directory) {
		it := d.Entries()

		for it.Next() {
			next, i := expect()

			if it.Name() != next.name {
				t.Fatalf("[%d] expected filename to be %q", i, next.name)
			}

			switch next.kind {
			case TFile:
				mf, ok := it.Node().(File)
				if !ok {
					t.Fatalf("[%d] expected file to be a normal file: %T", i, it.Node())
				}
				out, err := io.ReadAll(mf)
				if err != nil {
					t.Errorf("[%d] failed to read file", i)
					continue
				}
				if string(out) != next.value {
					t.Errorf(
						"[%d] while reading %q, expected %q, got %q",
						i,
						it.Name(),
						next.value,
						string(out),
					)
					continue
				}
			case TSymlink:
				mf, ok := it.Node().(*Symlink)
				if !ok {
					t.Errorf("[%d] expected file to be a symlink: %T", i, it.Node())
					continue
				}
				if mf.Target != next.value {
					t.Errorf(
						"[%d] target of symlink %q should have been %q but was %q",
						i,
						it.Name(),
						next.value,
						mf.Target,
					)
					continue
				}
			case TDirStart:
				mf, ok := it.Node().(Directory)
				if !ok {
					t.Fatalf(
						"[%d] expected file to be a directory: %T",
						i,
						it.Node(),
					)
				}
				check(mf)
			case TDirEnd:
				t.Errorf(
					"[%d] expected end of directory, found %#v at %q",
					i,
					it.Node(),
					it.Name(),
				)
				return
			default:
				t.Fatal("unhandled type", next.kind)
			}
			if err := it.Node().Close(); err != nil {
				t.Fatalf("[%d] expected to be able to close node", i)
			}
		}
		next, i := expect()

		if it.Err() != nil {
			t.Fatalf("[%d] got error: %s", i, it.Err())
		}

		if next.kind != TDirEnd {
			t.Fatalf("[%d] found end of directory, expected %#v", i, next)
		}
	}
	check(dir)
}
