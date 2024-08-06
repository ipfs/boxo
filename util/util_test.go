package util

import (
	"bytes"
	"testing"

	"github.com/ipfs/go-test/random"
)

func TestXOR(t *testing.T) {
	cases := [][3][]byte{
		{
			{0xFF, 0xFF, 0xFF},
			{0xFF, 0xFF, 0xFF},
			{0x00, 0x00, 0x00},
		},
		{
			{0x00, 0xFF, 0x00},
			{0xFF, 0xFF, 0xFF},
			{0xFF, 0x00, 0xFF},
		},
		{
			{0x55, 0x55, 0x55},
			{0x55, 0xFF, 0xAA},
			{0x00, 0xAA, 0xFF},
		},
	}

	for _, c := range cases {
		r := XOR(c[0], c[1])
		if !bytes.Equal(r, c[2]) {
			t.Error("XOR failed")
		}
	}
}

func BenchmarkHash256K(b *testing.B) {
	const size = 256 * 1024
	buf := random.Bytes(size)
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Hash(buf)
	}
}

func BenchmarkHash512K(b *testing.B) {
	const size = 512 * 1024
	buf := random.Bytes(size)
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Hash(buf)
	}
}

func BenchmarkHash1M(b *testing.B) {
	const size = 1024 * 1024
	buf := random.Bytes(size)
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Hash(buf)
	}
}
