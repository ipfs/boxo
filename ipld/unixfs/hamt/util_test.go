package hamt

import (
	"testing"
)

func TestHashBitsEvenSizes(t *testing.T) {
	buf := []byte{255, 127, 79, 45, 116, 99, 35, 17}
	hb := hashBits{b: buf}

	for _, v := range buf {
		if a, _ := hb.Next(8); a != int(v) {
			t.Fatalf("got wrong numbers back: expected %d, got %d", v, a)
		}
	}
}

func TestHashBitsOverflow(t *testing.T) {
	buf := []byte{255}
	hb := hashBits{b: buf}

	for i := 0; i < 8; i++ {
		bit, err := hb.Next(1)
		if err != nil {
			t.Fatalf("got %d bits back, expected 8: %s", i, err)
		}
		if bit != 1 {
			t.Fatal("expected all one bits")
		}
	}
	_, err := hb.Next(1)
	if err == nil {
		t.Error("overflowed the bit vector")
	}
}

func TestHashBitsUneven(t *testing.T) {
	buf := []byte{255, 127, 79, 45, 116, 99, 35, 17}
	hb := hashBits{b: buf}

	v, _ := hb.Next(4)
	if v != 15 {
		t.Fatal("should have gotten 15: ", v)
	}

	v, _ = hb.Next(4)
	if v != 15 {
		t.Fatal("should have gotten 15: ", v)
	}

	if v, _ := hb.Next(3); v != 3 {
		t.Fatalf("expected 3, but got %b", v)
	}
	if v, _ := hb.Next(3); v != 7 {
		t.Fatalf("expected 7, but got %b", v)
	}
	if v, _ := hb.Next(3); v != 6 {
		t.Fatalf("expected 6, but got %b", v)
	}

	if v, _ := hb.Next(15); v != 20269 {
		t.Fatalf("expected 20269, but got %b (%d)", v, v)
	}
}
