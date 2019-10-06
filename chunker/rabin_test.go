package chunk

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	util "github.com/ipfs/go-ipfs-util"
)

func TestRabinChunking(t *testing.T) {
	data := make([]byte, 1024*1024*16)
	util.NewTimeSeededRand().Read(data)

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

type cher interface {
	NextBytes() ([]byte, error)
}

type newChunker func(io.Reader) cher

func chunkData(t *testing.T, newC newChunker, data []byte) map[string]blocks.Block {
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

func testReuse(t *testing.T, cr newChunker) {
	data := make([]byte, 1024*1024*16)
	util.NewTimeSeededRand().Read(data)

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
	newRabin := func(r io.Reader) cher {
		return NewRabin(r, 256*1024)
	}
	testReuse(t, newRabin)
}

var Res uint64

func BenchmarkRabin(b *testing.B) {
	data := make([]byte, 16<<20)
	util.NewTimeSeededRand().Read(data)

	b.SetBytes(16 << 20)
	b.ReportAllocs()
	b.ResetTimer()

	var res uint64

	for i := 0; i < b.N; i++ {
		r := NewRabin(bytes.NewReader(data), 1024*256)

		for {
			chunk, err := r.NextBytes()
			if err != nil {
				if err == io.EOF {
					break
				}
				b.Fatal(err)
			}
			res = res + uint64(len(chunk))
		}
	}
	Res = Res + res
}
