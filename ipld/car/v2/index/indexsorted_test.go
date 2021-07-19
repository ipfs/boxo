package index

import (
	"testing"

	"github.com/ipfs/go-merkledag"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestSortedIndexCodec(t *testing.T) {
	require.Equal(t, multicodec.CarIndexSorted, newSorted().Codec())
}

func TestSortedIndex_GetReturnsNotFoundWhenCidDoesNotExist(t *testing.T) {
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
