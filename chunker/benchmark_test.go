package chunk

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
)

type newSplitter func(io.Reader) Splitter

type bencSpec struct {
	size int
	name string
}

var bSizes = []bencSpec{
	{1 << 10, "1K"},
	{1 << 20, "1M"},
	{16 << 20, "16M"},
	{100 << 20, "100M"},
}

func benchmarkChunker(b *testing.B, ns newSplitter) {
	for _, s := range bSizes {
		s := s
		b.Run(s.name, func(b *testing.B) {
			benchmarkChunkerSize(b, ns, s.size)
		})
	}
}

func benchmarkChunkerSize(b *testing.B, ns newSplitter, size int) {
	rng := rand.New(rand.NewSource(1))
	data := make([]byte, size)
	rng.Read(data)

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	var res uint64

	for i := 0; i < b.N; i++ {
		r := ns(bytes.NewReader(data))

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

func benchmarkFilesAlloc(b *testing.B, ns newSplitter) {
	const (
		chunkSize   = 4096
		minDataSize = 20000
		maxDataSize = 60000
		fileCount   = 10000
	)
	rng := rand.New(rand.NewSource(1))
	data := make([]byte, maxDataSize)
	rng.Read(data)

	b.SetBytes(maxDataSize)
	b.ReportAllocs()
	b.ResetTimer()

	var res uint64

	for i := 0; i < b.N; i++ {
		for j := 0; j < fileCount; j++ {
			fileSize := rng.Intn(maxDataSize-minDataSize) + minDataSize
			r := ns(bytes.NewReader(data[:fileSize]))
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
	}
	Res = Res + res
}
