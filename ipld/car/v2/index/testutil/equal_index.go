package testutil

import (
	"sync"
	"testing"

	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

type Index interface {
	ForEach(func(multihash.Multihash, uint64) error) error
}

// insertUint64 perform one round of insertion sort on the last element
func insertUint64(s []uint64) {
	switch len(s) {
	case 0, 1:
		return
	default:
		cur := s[len(s)-1]
		for j := len(s) - 1; j > 0; {
			j--
			if cur >= s[j] {
				s[j+1] = cur
				break
			}
			s[j+1] = s[j]
		}
	}
}

func AssertIndenticalIndexes(t *testing.T, a, b Index) {
	var wg sync.Mutex
	wg.Lock()
	// key is multihash.Multihash.HexString
	var aCount uint
	aMap := make(map[string][]uint64)
	go func() {
		defer wg.Unlock()
		a.ForEach(func(mh multihash.Multihash, off uint64) error {
			aCount++
			str := mh.HexString()
			slice, _ := aMap[str]
			slice = append(slice, off)
			insertUint64(slice)
			aMap[str] = slice
			return nil
		})
	}()

	var bCount uint
	bMap := make(map[string][]uint64)
	a.ForEach(func(mh multihash.Multihash, off uint64) error {
		bCount++
		str := mh.HexString()
		slice, _ := bMap[str]
		slice = append(slice, off)
		insertUint64(slice)
		bMap[str] = slice
		return nil
	})
	wg.Lock()

	require.Equal(t, aCount, bCount)
	require.Equal(t, aMap, bMap)
}
