package chunk

import (
	"bytes"
	"io"
	"testing"

	random "github.com/ipfs/go-test/random"
)

func testBuzhashChunking(t *testing.T, buf []byte) (chunkCount int) {
	t.Parallel()

	n, err := random.NewRand().Read(buf)
	if n < len(buf) {
		t.Fatalf("expected %d bytes, got %d", len(buf), n)
	}
	if err != nil {
		t.Fatal(err)
	}

	r := NewBuzhash(bytes.NewReader(buf))

	var chunks [][]byte

	for {
		chunk, err := r.NextBytes()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}

		chunks = append(chunks, chunk)
	}
	chunkCount += len(chunks)

	for i, chunk := range chunks {
		if len(chunk) == 0 {
			t.Fatalf("chunk %d/%d is empty", i+1, len(chunks))
		}
	}

	for i, chunk := range chunks[:len(chunks)-1] {
		if len(chunk) < buzMin {
			t.Fatalf("chunk %d/%d is less than the minimum size", i+1, len(chunks))
		}
	}

	unchunked := bytes.Join(chunks, nil)
	if !bytes.Equal(unchunked, buf) {
		t.Fatal("data was chunked incorrectly")
	}

	return chunkCount
}

func TestBuzhashChunking(t *testing.T) {
	buf := make([]byte, 1024*1024*16)
	count := testBuzhashChunking(t, buf)
	t.Logf("average block size: %d\n", len(buf)/count)
}

func TestBuzhashChunkReuse(t *testing.T) {
	newBuzhash := func(r io.Reader) Splitter {
		return NewBuzhash(r)
	}
	testReuse(t, newBuzhash)
}

func BenchmarkBuzhash2(b *testing.B) {
	benchmarkChunker(b, func(r io.Reader) Splitter {
		return NewBuzhash(r)
	})
}

func TestBuzhashBitsHashBias(t *testing.T) {
	counts := make([]byte, 32)
	for _, h := range bytehash {
		for i := 0; i < 32; i++ {
			if h&1 == 1 {
				counts[i]++
			}
			h = h >> 1
		}
	}
	for i, c := range counts {
		if c != 128 {
			t.Errorf("Bit balance in position %d broken, %d ones", i, c)
		}
	}
}

func FuzzBuzhashChunking(f *testing.F) {
	f.Add(make([]byte, 1024*1024*16))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) < buzMin {
			return
		}
		testBuzhashChunking(t, b)
	})
}
