package index

import (
	"encoding/binary"
	"testing"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestSortedIndexCodec(t *testing.T) {
	require.Equal(t, multicodec.CarIndexSorted, newSorted().Codec())
}

func TestIndexSorted_GetReturnsNotFoundWhenCidDoesNotExist(t *testing.T) {
	nonExistingKey := merkledag.NewRawNode([]byte("lobstermuncher")).Block.Cid()
	tests := []struct {
		name    string
		subject Index
	}{
		{
			"Sorted",
			newSorted(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOffset, err := GetFirst(tt.subject, nonExistingKey)
			require.Equal(t, ErrNotFound, err)
			require.Equal(t, uint64(0), gotOffset)
		})
	}
}

func TestSingleWidthIndex_GetAll(t *testing.T) {
	l := 4
	width := 9
	buf := make([]byte, width*l)

	// Populate the index bytes as total of four records.
	// The last record should not match the getAll.
	for i := 0; i < l; i++ {
		if i < l-1 {
			buf[i*width] = 1
		} else {
			buf[i*width] = 2
		}
		binary.LittleEndian.PutUint64(buf[(i*width)+1:(i*width)+width], uint64(14))
	}
	subject := &singleWidthIndex{
		width: 9,
		len:   uint64(l),
		index: buf,
	}

	var foundCount int
	err := subject.getAll([]byte{1}, func(u uint64) bool {
		foundCount++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, 3, foundCount)
}
