//go:build go1.18

package car_test

import (
	"bytes"
	"encoding/hex"
	"io"
	"testing"

	car "github.com/ipfs/boxo/ipld/car"
)

func FuzzCarReader(f *testing.F) {
	fixture, err := hex.DecodeString(fixtureStr)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(fixture)

	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := car.NewCarReader(bytes.NewReader(data))
		if err != nil {
			return
		}

		for {
			_, err = r.Next()
			if err == io.EOF {
				return
			}
		}
	})
}
