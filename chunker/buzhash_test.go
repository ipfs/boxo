package chunk

import (
	"bytes"
	"io"
	"testing"

	util "github.com/ipfs/go-ipfs-util"
)

func TestBuzhashChunking(t *testing.T) {
	data := make([]byte, 1024*1024*16)

	chunkCount := 0
	rounds := 100

	for i := 0; i < rounds; i++ {
		util.NewTimeSeededRand().Read(data)

		r := NewBuzhash(bytes.NewReader(data))

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
		if !bytes.Equal(unchunked, data) {
			t.Fatal("data was chunked incorrectly")
		}
	}
	t.Logf("average block size: %d\n", len(data)*rounds/chunkCount)
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
