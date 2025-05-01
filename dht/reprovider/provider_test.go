package reprovider

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/probe-lab/go-libdht/kad/key/bit256"
	"github.com/probe-lab/go-libdht/kad/key/bitstr"
	"github.com/probe-lab/go-libdht/kad/trie"
	"github.com/stretchr/testify/require"
)

func TestNextNonEmptyLeafFullTrie(t *testing.T) {
	bitlen := 4

	tr := trie.New[bitstr.Key, any]()
	nKeys := 1 << bitlen
	binaryKeys := make([]bitstr.Key, 0, nKeys)
	for i := range nKeys {
		binary := fmt.Sprintf("%0*b", bitlen, i)
		k := bitstr.Key(binary)
		tr.Add(k, struct{}{})
		binaryKeys = append(binaryKeys, k)
	}

	order := binaryKeys[0]
	t.Run("OrderZero", func(t *testing.T) {
		for i, k := range binaryKeys {
			nextKey := nextNonEmptyLeaf(tr, k, order).Key
			require.Equal(t, binaryKeys[(i+1)%nKeys], nextKey)
		}
	})

	t.Run("Cycle", func(t *testing.T) {
		initialKey := binaryKeys[0]
		k := initialKey
		for range binaryKeys {
			k = nextNonEmptyLeaf(tr, k, order).Key
		}
		require.Equal(t, initialKey, k)
	})

	order = binaryKeys[nKeys-1]
	t.Run("CustomOrder", func(t *testing.T) {
		for i, k := range binaryKeys {
			nextKey := nextNonEmptyLeaf(tr, k, order).Key
			require.Equal(t, binaryKeys[(i-1+nKeys)%nKeys], nextKey)
		}
	})
}

func TestNextNonEmptyLeafSparseTrie(t *testing.T) {
	bitlen := 10
	sparsity := 4

	tr := trie.New[bitstr.Key, any]()
	nKeys := 1 << (bitlen - sparsity)
	binaryKeys := make([]bitstr.Key, 0, nKeys)
	suffix := (1 << sparsity) - 1
	for i := range nKeys {
		binary := fmt.Sprintf("%0*b", bitlen, i*(1<<sparsity)+suffix)
		k := bitstr.Key(binary)
		tr.Add(k, struct{}{})
		binaryKeys = append(binaryKeys, k)
	}

	order := bitstr.Key(fmt.Sprintf("%0*b", bitlen, 0))
	t.Run("OrderZero", func(t *testing.T) {
		for i, k := range binaryKeys {
			nextKey := nextNonEmptyLeaf(tr, k, order).Key
			require.Equal(t, binaryKeys[(i+1)%nKeys], nextKey)
		}
	})

	t.Run("MissingKey", func(t *testing.T) {
		for i := range 1 << bitlen {
			binary := fmt.Sprintf("%0*b", bitlen, i)
			k := bitstr.Key(binary)
			nextKey := nextNonEmptyLeaf(tr, k, order).Key
			require.Equal(t, binaryKeys[((i+1)%(1<<bitlen))/(1<<sparsity)], nextKey, k)
		}
	})

	order = binaryKeys[nKeys-1]
	t.Run("CustomOrder", func(t *testing.T) {
		for i, k := range binaryKeys {
			nextKey := nextNonEmptyLeaf(tr, k, order).Key
			require.Equal(t, binaryKeys[(i-1+nKeys)%nKeys], nextKey)
		}
	})
}

func TestReprovideTimeForPrefixWithOrderZero(t *testing.T) {
	s := reprovideSweeper{
		reprovideInterval: 16 * time.Second,
		order:             bit256.ZeroKey(),
	}

	require.Equal(t, 0*time.Second, s.reprovideTimeForPrefix("0"))
	require.Equal(t, 8*time.Second, s.reprovideTimeForPrefix("1"))
	require.Equal(t, 0*time.Second, s.reprovideTimeForPrefix("000"))
	require.Equal(t, 8*time.Second, s.reprovideTimeForPrefix("1000"))
	require.Equal(t, 10*time.Second, s.reprovideTimeForPrefix("1010"))
	require.Equal(t, 15*time.Second, s.reprovideTimeForPrefix("1111"))
}

func TestReprovideTimeForPrefixWithCustomOrder(t *testing.T) {
	s := reprovideSweeper{
		reprovideInterval: 16 * time.Second,
		order:             bit256.NewKey(bytes.Repeat([]byte{0xFF}, 32)), // 111...1
	}

	require.Equal(t, 0*time.Second, s.reprovideTimeForPrefix("1"))
	require.Equal(t, 8*time.Second, s.reprovideTimeForPrefix("0"))
	require.Equal(t, 0*time.Second, s.reprovideTimeForPrefix("111"))
	require.Equal(t, 8*time.Second, s.reprovideTimeForPrefix("0111"))
	require.Equal(t, 10*time.Second, s.reprovideTimeForPrefix("0101"))
	require.Equal(t, 15*time.Second, s.reprovideTimeForPrefix("0000"))
}

func TestKeyToBytes(t *testing.T) {
	require.Equal(t, []byte{0b00000000}, keyToBytes(bitstr.Key("0")))
	require.Equal(t, []byte{0b00000000}, keyToBytes(bitstr.Key("00000000")))
	require.Equal(t, []byte{0b00000000, 0b00000000}, keyToBytes(bitstr.Key("000000000")))
	require.Equal(t, []byte{0b00110000}, keyToBytes(bitstr.Key("0011")))
	require.Equal(t, []byte{0b11111110}, keyToBytes(bitstr.Key("1111111")))
}

func TestShortestCoveredPrefix(t *testing.T) {
	// TODO:
}

// TODO: Test that the peers returned by GetClosestPeers are sorted by distance to the requested key
