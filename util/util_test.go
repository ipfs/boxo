package util_test

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"testing"

	"github.com/ipfs/boxo/util"
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
		r := util.XOR(c[0], c[1])
		if !bytes.Equal(r, c[2]) {
			t.Error("XOR failed")
		}
	}
}

func BenchmarkHash256K(b *testing.B) {
	const size = 256 * 1024
	buf := randomBytes(size)
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		util.Hash(buf)
	}
}

func BenchmarkHash512K(b *testing.B) {
	const size = 512 * 1024
	buf := randomBytes(size)
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		util.Hash(buf)
	}
}

func BenchmarkHash1M(b *testing.B) {
	const size = 1024 * 1024
	buf := randomBytes(size)
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		util.Hash(buf)
	}
}

func randomBytes(n int) []byte {
	var seed [32]byte
	binary.BigEndian.PutUint64(seed[:], uint64(n))
	cc8 := rand.NewChaCha8(seed)
	data := make([]byte, n)
	cc8.Read(data)
	return data
}
