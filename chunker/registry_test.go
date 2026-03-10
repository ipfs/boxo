package chunk_test

import (
	"bytes"
	"io"
	"testing"

	chunk "github.com/ipfs/boxo/chunker"
)

type noSplits struct {
	r       io.Reader
	drained bool
}

func (ns *noSplits) Reader() io.Reader {
	return ns.r
}

func (ns *noSplits) NextBytes() ([]byte, error) {
	if ns.drained {
		return nil, io.EOF
	}
	ns.drained = true
	return io.ReadAll(ns.r)
}

// TestRegister is not parallel because Register mutates package-level state
// and panics on duplicate names.
func TestRegister(t *testing.T) {
	t.Run("name only", func(t *testing.T) {
		chunk.Register("mockplain", func(r io.Reader, _ string) (chunk.Splitter, error) {
			return &noSplits{r: r}, nil
		})
		r := bytes.NewReader([]byte{1, 2, 3})
		s, err := chunk.FromString(r, "mockplain")
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := s.(*noSplits); !ok {
			t.Fatal("unexpected splitter type")
		}
	})

	t.Run("name and params", func(t *testing.T) {
		const chunkerStr = "mockparams-123"
		chunk.Register("mockparams", func(r io.Reader, c string) (chunk.Splitter, error) {
			if c != chunkerStr {
				t.Fatalf("expected chunker string %q, got %q", chunkerStr, c)
			}
			return &noSplits{r: r}, nil
		})
		r := bytes.NewReader([]byte{1, 2, 3})
		s, err := chunk.FromString(r, chunkerStr)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := s.(*noSplits); !ok {
			t.Fatal("unexpected splitter type")
		}
	})

	t.Run("unregistered name", func(t *testing.T) {
		r := bytes.NewReader([]byte{1, 2, 3})
		_, err := chunk.FromString(r, "nonexistent")
		if err == nil {
			t.Fatal("expected error for unregistered chunker")
		}
	})

	t.Run("panic on empty name", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic for empty name")
			}
		}()
		chunk.Register("", func(r io.Reader, _ string) (chunk.Splitter, error) {
			return nil, nil
		})
	})

	t.Run("panic on name with dash", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic for name with dash")
			}
		}()
		chunk.Register("my-format", func(r io.Reader, _ string) (chunk.Splitter, error) {
			return nil, nil
		})
	})

	t.Run("panic on nil ctor", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic for nil ctor")
			}
		}()
		chunk.Register("nilctor", nil)
	})

	t.Run("panic on duplicate name", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic for duplicate name")
			}
		}()
		// "mockplain" was already registered in "name only" subtest.
		chunk.Register("mockplain", func(r io.Reader, _ string) (chunk.Splitter, error) {
			return nil, nil
		})
	})
}
