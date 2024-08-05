package chunk

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/ipfs/go-test/random"
)

func randBuf(t *testing.T, size int) []byte {
	buf := make([]byte, size)
	if _, err := random.NewRand().Read(buf); err != nil {
		t.Fatal("failed to read enough randomness")
	}
	return buf
}

func copyBuf(buf []byte) []byte {
	cpy := make([]byte, len(buf))
	copy(cpy, buf)
	return cpy
}

func TestSizeSplitterOverAllocate(t *testing.T) {
	t.Parallel()

	const max = 1000
	r := bytes.NewReader(randBuf(t, max))
	chunksize := int64(1024 * 256)
	splitter := NewSizeSplitter(r, chunksize)
	chunk, err := splitter.NextBytes()
	if err != nil {
		t.Fatal(err)
	}
	if cap(chunk)-len(chunk) > maxOverAllocBytes {
		t.Fatal("chunk capacity too large")
	}
}

func TestSizeSplitterIsDeterministic(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	test := func() {
		bufR := randBuf(t, 10000000) // crank this up to satisfy yourself.
		bufA := copyBuf(bufR)
		bufB := copyBuf(bufR)

		chunksA, _ := Chan(DefaultSplitter(bytes.NewReader(bufA)))
		chunksB, _ := Chan(DefaultSplitter(bytes.NewReader(bufB)))

		for n := 0; ; n++ {
			a, moreA := <-chunksA
			b, moreB := <-chunksB

			if !moreA {
				if moreB {
					t.Fatal("A ended, B didnt.")
				}
				return
			}

			if !bytes.Equal(a, b) {
				t.Fatalf("chunk %d not equal", n)
			}
		}
	}

	for run := 0; run < 1; run++ { // crank this up to satisfy yourself.
		test()
	}
}

func TestSizeSplitterFillsChunks(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	const max = 10000000
	b := randBuf(t, max)
	r := &clipReader{r: bytes.NewReader(b), size: 4000}
	const chunksize = 1024 * 256
	c, _ := Chan(NewSizeSplitter(r, chunksize))

	sofar := 0
	whole := make([]byte, max)
	for chunk := range c {
		bc := b[sofar : sofar+len(chunk)]
		if !bytes.Equal(bc, chunk) {
			t.Fatalf("chunk not correct: (sofar: %d) %d != %d, %v != %v", sofar, len(bc), len(chunk), bc[:100], chunk[:100])
		}

		copy(whole[sofar:], chunk)

		sofar += len(chunk)
		if sofar != max && len(chunk) < chunksize {
			t.Fatal("sizesplitter split at a smaller size")
		}
	}

	if !bytes.Equal(b, whole) {
		t.Fatal("splitter did not split right")
	}
}

type clipReader struct {
	size int
	r    io.Reader
}

func (s *clipReader) Read(buf []byte) (int, error) {
	// clip the incoming buffer to produce smaller chunks
	if len(buf) > s.size {
		buf = buf[:s.size]
	}

	return s.r.Read(buf)
}

func BenchmarkDefault(b *testing.B) {
	benchmarkChunker(b, func(r io.Reader) Splitter {
		return DefaultSplitter(r)
	})
}

// BenchmarkFilesAllocPool benchmarks splitter that uses go-buffer-pool,
// simulating use in unixfs with many small files.
func BenchmarkFilesAllocPool(b *testing.B) {
	const fileBlockSize = 4096

	benchmarkFilesAlloc(b, func(r io.Reader) Splitter {
		return NewSizeSplitter(r, fileBlockSize)
	})
}

// BenchmarkFilesAllocPool benchmarks splitter that does not use
// go-buffer-pool, simulating use in unixfs with many small files.
func BenchmarkFilesAllocNoPool(b *testing.B) {
	const fileBlockSize = 4096

	benchmarkFilesAlloc(b, func(r io.Reader) Splitter {
		return &sizeSplitterNoPool{
			r:    r,
			size: uint32(fileBlockSize),
		}
	})
}

// sizeSplitterNoPool implements Splitter that allocates without pool. Provided
// for benchmarking against implementation with pool.
type sizeSplitterNoPool struct {
	r    io.Reader
	size uint32
	err  error
}

func (ss *sizeSplitterNoPool) NextBytes() ([]byte, error) {
	if ss.err != nil {
		return nil, ss.err
	}

	full := make([]byte, ss.size)
	n, err := io.ReadFull(ss.r, full)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			ss.err = io.EOF
			if n == 0 {
				return nil, nil
			}
			small := make([]byte, n)
			copy(small, full)
			return small, nil
		}
		return nil, err
	}
	return full, nil
}

func (ss *sizeSplitterNoPool) Reader() io.Reader {
	return ss.r
}
