//go:build !race
// +build !race

package chunk

import (
	"testing"
)

func TestFuzzBuzhashChunking(t *testing.T) {
	buf := make([]byte, 1024*1024*16)
	for i := 0; i < 100; i++ {
		testBuzhashChunking(t, buf)
	}
}
