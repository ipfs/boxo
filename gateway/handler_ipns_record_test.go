package gateway

import (
	"crypto/rand"
	"testing"
	"time"

	ipnstest "github.com/ipfs/boxo/internal/ipnstest"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/stretchr/testify/require"
)

func TestIPNSRecordMaxAge(t *testing.T) {
	t.Parallel()

	sk, _, err := ci.GenerateKeyPairWithReader(ci.Ed25519, 2048, rand.Reader)
	require.NoError(t, err)

	value, err := path.NewPath("/ipfs/bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	require.NoError(t, err)

	makeRecord := func(t *testing.T, ttl time.Duration, eol time.Time) *ipns.Record {
		rec, err := ipns.NewRecord(sk, value, 1, eol, ttl)
		require.NoError(t, err)
		return rec
	}

	t.Run("ttl below remaining validity is used as-is", func(t *testing.T) {
		t.Parallel()
		rec := makeRecord(t, time.Minute, time.Now().Add(time.Hour))
		maxAge, ok := ipnsRecordMaxAge(rec)
		require.True(t, ok)
		require.Equal(t, time.Minute, maxAge)
	})

	t.Run("ttl above remaining validity is clamped to EOL", func(t *testing.T) {
		t.Parallel()
		rec := makeRecord(t, time.Hour, time.Now().Add(30*time.Second))
		maxAge, ok := ipnsRecordMaxAge(rec)
		require.True(t, ok)
		require.Greater(t, maxAge, time.Duration(0))
		require.LessOrEqual(t, maxAge, 30*time.Second)
	})

	t.Run("expired record yields max-age=0", func(t *testing.T) {
		t.Parallel()
		rec := makeRecord(t, time.Hour, time.Now().Add(-time.Hour))
		maxAge, ok := ipnsRecordMaxAge(rec)
		require.True(t, ok)
		require.Equal(t, time.Duration(0), maxAge)
	})

	t.Run("negative ttl is floored to max-age=0", func(t *testing.T) {
		t.Parallel()
		// Built at the wire level so the record carries a genuinely negative TTL
		// that ipns.NewRecord would otherwise floor.
		rec, err := ipnstest.RawRecordWithTTL(value, time.Now().Add(time.Hour), -time.Minute)
		require.NoError(t, err)
		maxAge, ok := ipnsRecordMaxAge(rec)
		require.True(t, ok)
		require.Equal(t, time.Duration(0), maxAge)
	})
}
