package chunk

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-test/random"
)

func TestRabinChunking(t *testing.T) {
	t.Parallel()

	data := make([]byte, 1024*1024*16)
	n, err := random.NewRand().Read(data)
	if n < len(data) {
		t.Fatalf("expected %d bytes, got %d", len(data), n)
	}
	if err != nil {
		t.Fatal(err)
	}

	r := NewRabin(bytes.NewReader(data), 1024*256)

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

	fmt.Printf("average block size: %d\n", len(data)/len(chunks))

	unchunked := bytes.Join(chunks, nil)
	if !bytes.Equal(unchunked, data) {
		fmt.Printf("%d %d\n", len(unchunked), len(data))
		t.Fatal("data was chunked incorrectly")
	}
}

func chunkData(t *testing.T, newC newSplitter, data []byte) map[string]blocks.Block {
	r := newC(bytes.NewReader(data))

	blkmap := make(map[string]blocks.Block)

	for {
		blk, err := r.NextBytes()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}

		b := blocks.NewBlock(blk)
		blkmap[b.Cid().KeyString()] = b
	}

	return blkmap
}

func testReuse(t *testing.T, cr newSplitter) {
	t.Parallel()

	data := make([]byte, 1024*1024*16)
	n, err := random.NewRand().Read(data)
	if n < len(data) {
		t.Fatalf("expected %d bytes, got %d", len(data), n)
	}
	if err != nil {
		t.Fatal(err)
	}

	ch1 := chunkData(t, cr, data[1000:])
	ch2 := chunkData(t, cr, data)

	var extra int
	for k := range ch2 {
		_, ok := ch1[k]
		if !ok {
			extra++
		}
	}

	if extra > 2 {
		t.Logf("too many spare chunks made: %d", extra)
	}
}

func TestRabinChunkReuse(t *testing.T) {
	newRabin := func(r io.Reader) Splitter {
		return NewRabin(r, 256*1024)
	}
	testReuse(t, newRabin)
}

var Res uint64

func BenchmarkRabin(b *testing.B) {
	benchmarkChunker(b, func(r io.Reader) Splitter {
		return NewRabin(r, 256<<10)
	})
}
