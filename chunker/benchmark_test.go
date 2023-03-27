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
