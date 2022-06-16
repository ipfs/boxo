//go:build go1.18
// +build go1.18

package car_test

import (
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	car "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
)

// v1FixtureStr is a clean carv1 single-block, single-root CAR
const v1FixtureStr = "3aa265726f6f747381d82a58250001711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80b6776657273696f6e012c01711220151fe9e73c6267a7060c6f6c4cca943c236f4b196723489608edb42a8b8fa80ba165646f646779f5"

func seedWithCarFiles(f *testing.F) {
	fixture, err := hex.DecodeString(v1FixtureStr)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(fixture)
	files, err := filepath.Glob("testdata/*.car")
	if err != nil {
		f.Fatal(err)
	}
	for _, fname := range files {
		func() {
			file, err := os.Open(fname)
			if err != nil {
				f.Fatal(err)
			}
			defer file.Close()
			data, err := io.ReadAll(file)
			if err != nil {
				f.Fatal(err)
			}
			f.Add(data)
		}()
	}
}

func FuzzBlockReader(f *testing.F) {
	seedWithCarFiles(f)

	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := car.NewBlockReader(bytes.NewReader(data))
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

func FuzzReader(f *testing.F) {
	seedWithCarFiles(f)

	f.Fuzz(func(t *testing.T, data []byte) {
		subject, err := car.NewReader(bytes.NewReader(data))
		if err != nil {
			return
		}

		subject.Roots()
		ir := subject.IndexReader()
		if ir != nil {
			index.ReadFrom(ir)
		}
		car.GenerateIndex(subject.DataReader())
	})
}
