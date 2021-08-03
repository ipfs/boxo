package car_test

import (
	"io"
	"os"
	"testing"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/internal/carv1"
	"github.com/stretchr/testify/require"
)

func TestBlockReaderFailsOnUnknownVersion(t *testing.T) {
	r := requireReaderFromPath(t, "testdata/sample-rootless-v42.car")
	_, err := carv2.NewBlockReader(r)
	require.EqualError(t, err, "invalid car version: 42")
}

func TestBlockReaderFailsOnCorruptPragma(t *testing.T) {
	r := requireReaderFromPath(t, "testdata/sample-corrupt-pragma.car")
	_, err := carv2.NewBlockReader(r)
	require.EqualError(t, err, "unexpected EOF")
}

func TestBlockReader_WithCarV1Consistency(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		zerLenAsEOF bool
		wantVersion uint64
	}{
		{
			name:        "CarV1WithoutZeroLengthSection",
			path:        "testdata/sample-v1.car",
			wantVersion: 1,
		},
		{
			name:        "CarV1WithZeroLenSection",
			path:        "testdata/sample-v1-with-zero-len-section.car",
			zerLenAsEOF: true,
			wantVersion: 1,
		},
		{
			name:        "AnotherCarV1WithZeroLenSection",
			path:        "testdata/sample-v1-with-zero-len-section2.car",
			zerLenAsEOF: true,
			wantVersion: 1,
		},
		{
			name:        "CarV1WithZeroLenSectionWithoutOption",
			path:        "testdata/sample-v1-with-zero-len-section.car",
			wantVersion: 1,
		},
		{
			name:        "AnotherCarV1WithZeroLenSectionWithoutOption",
			path:        "testdata/sample-v1-with-zero-len-section2.car",
			wantVersion: 1,
		},
		{
			name:        "CorruptCarV1",
			path:        "testdata/sample-v1-tailing-corrupt-section.car",
			wantVersion: 1,
		},
		{
			name:        "CarV2WrappingV1",
			path:        "testdata/sample-wrapped-v2.car",
			wantVersion: 2,
		},
		{
			name:        "CarV2ProducedByBlockstore",
			path:        "testdata/sample-rw-bs-v2.car",
			wantVersion: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := requireReaderFromPath(t, tt.path)
			subject, err := carv2.NewBlockReader(r, carv2.ZeroLengthSectionAsEOF(tt.zerLenAsEOF))
			require.NoError(t, err)

			require.Equal(t, tt.wantVersion, subject.Version)

			var wantReader *carv1.CarReader
			switch tt.wantVersion {
			case 1:
				wantReader = requireNewCarV1ReaderFromV1File(t, tt.path, tt.zerLenAsEOF)
			case 2:
				wantReader = requireNewCarV1ReaderFromV2File(t, tt.path, tt.zerLenAsEOF)
			default:
				require.Failf(t, "invalid test-case", "unknown wantVersion %v", tt.wantVersion)
			}
			require.Equal(t, wantReader.Header.Roots, subject.Roots)

			for {
				gotBlock, gotErr := subject.Next()
				wantBlock, wantErr := wantReader.Next()
				require.Equal(t, wantBlock, gotBlock)
				require.Equal(t, wantErr, gotErr)
				if gotErr == io.EOF {
					break
				}
			}
		})
	}
}

func requireReaderFromPath(t *testing.T, path string) io.Reader {
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	return f
}
