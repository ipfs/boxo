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

func (ms *noSplits) Reader() io.Reader {
	return ms.r
}

func (ms *noSplits) NextBytes() ([]byte, error) {
	if ms.drained {
		return nil, io.EOF
	}
	ms.drained = true
	return io.ReadAll(ms.r)
}

func TestRegister(t *testing.T) {
	t.Run("name only", func(t *testing.T) {
		name := "mock"
		chunker := name
		chunk.Register(name, func(r io.Reader, _ string) (chunk.Splitter, error) {
			mockSplitter := noSplits{r: r}
			return &mockSplitter, nil
		})
		r := bytes.NewReader([]byte{1, 2, 3})
		s, err := chunk.FromString(r, chunker)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := s.(*noSplits); !ok {
			t.Fatal("unexpected reader")
		}
	})

	t.Run("name and params", func(t *testing.T) {
		name := "mock"
		chunker := "mock-123"
		chunk.Register(name, func(r io.Reader, c string) (chunk.Splitter, error) {
			if c != chunker {
				t.Fatal("chunker string did not contain params")
			}
			mockSplitter := noSplits{r: r}
			return &mockSplitter, nil
		})
		r := bytes.NewReader([]byte{1, 2, 3})
		s, err := chunk.FromString(r, chunker)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := s.(*noSplits); !ok {
			t.Fatal("unexpected reader")
		}
	})
}
