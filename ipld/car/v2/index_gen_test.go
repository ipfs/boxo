package car

import (
	"os"
	"testing"

	"github.com/ipld/go-car/v2/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadOrGenerateIndex(t *testing.T) {
	tests := []struct {
		name        string
		carPath     string
		wantIndexer func(t *testing.T) index.Index
		wantErr     bool
	}{
		{
			"CarV1IsIndexedAsExpected",
			"testdata/sample-v1.car",
			func(t *testing.T) index.Index {
				v1, err := os.Open("testdata/sample-v1.car")
				require.NoError(t, err)
				defer v1.Close()
				want, err := GenerateIndex(v1)
				require.NoError(t, err)
				return want
			},
			false,
		},
		{
			"CarV2WithIndexIsReturnedAsExpected",
			"testdata/sample-wrapped-v2.car",
			func(t *testing.T) index.Index {
				v2, err := os.Open("testdata/sample-wrapped-v2.car")
				require.NoError(t, err)
				defer v2.Close()
				reader, err := NewReader(v2)
				require.NoError(t, err)
				want, err := index.ReadFrom(reader.IndexReader())
				require.NoError(t, err)
				return want
			},
			false,
		},
		{
			"CarOtherThanV1OrV2IsError",
			"testdata/sample-rootless-v42.car",
			func(t *testing.T) index.Index { return nil },
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			carFile, err := os.Open(tt.carPath)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, carFile.Close()) })
			got, err := ReadOrGenerateIndex(carFile)
			if tt.wantErr {
				require.Error(t, err)
			}
			want := tt.wantIndexer(t)
			require.Equal(t, want, got)
		})
	}
}

func TestGenerateIndexFromFile(t *testing.T) {
	tests := []struct {
		name        string
		carPath     string
		wantIndexer func(t *testing.T) index.Index
		wantErr     bool
	}{
		{
			"CarV1IsIndexedAsExpected",
			"testdata/sample-v1.car",
			func(t *testing.T) index.Index {
				v1, err := os.Open("testdata/sample-v1.car")
				require.NoError(t, err)
				defer v1.Close()
				want, err := GenerateIndex(v1)
				require.NoError(t, err)
				return want
			},
			false,
		},
		{
			"CarV2IsErrorSinceOnlyV1PayloadIsExpected",
			"testdata/sample-wrapped-v2.car",
			func(t *testing.T) index.Index { return nil },
			true,
		},
		{
			"CarOtherThanV1OrV2IsError",
			"testdata/sample-rootless-v42.car",
			func(t *testing.T) index.Index { return nil },
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateIndexFromFile(tt.carPath)
			if tt.wantErr {
				require.Error(t, err)
			}
			want := tt.wantIndexer(t)
			require.Equal(t, want, got)
		})
	}
}
