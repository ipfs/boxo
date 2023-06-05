package util_test

import (
	"bytes"
	crand "crypto/rand"
	"math/rand"
	"testing"

	"github.com/ipfs/boxo/ipld/car/util"
	"github.com/stretchr/testify/require"
)

func TestLdSize(t *testing.T) {
	for i := 0; i < 5; i++ {
		var buf bytes.Buffer
		data := make([][]byte, 5)
		for j := 0; j < 5; j++ {
			data[j] = make([]byte, rand.Intn(30))
			_, err := crand.Read(data[j])
			require.NoError(t, err)
		}
		size := util.LdSize(data...)
		err := util.LdWrite(&buf, data...)
		require.NoError(t, err)
		require.Equal(t, uint64(len(buf.Bytes())), size)
	}
}
