package ipns

import (
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/boxo/path"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func shuffle[T any](a []T) {
	for n := 0; n < 5; n++ {
		for i := range a {
			j := rand.Intn(len(a))
			a[i], a[j] = a[j], a[i]
		}
	}
}

func TestOrdering(t *testing.T) {
	t.Parallel()

	sk, _, _ := mustKeyPair(t, ic.Ed25519)
	ts := time.Unix(1000000, 0)

	assert := func(name string, r *Record, from ...*Record) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			shuffle(from)
			var vals [][]byte
			for _, r := range from {
				data, err := MarshalRecord(r)
				require.NoError(t, err)
				vals = append(vals, data)
			}

			i, err := selectRecord(from, vals)
			require.NoError(t, err)
			require.Equal(t, r, from[i], "selected incorrect record")
		})
	}

	r1, err := NewRecord(sk, testPath, 1, ts.Add(time.Hour), 0)
	require.NoError(t, err)

	r2, err := NewRecord(sk, testPath, 2, ts.Add(time.Hour), 0)
	require.NoError(t, err)

	r3, err := NewRecord(sk, testPath, 3, ts.Add(time.Hour), 0)
	require.NoError(t, err)

	r4, err := NewRecord(sk, testPath, 3, ts.Add(time.Hour*2), 0)
	require.NoError(t, err)

	r5, err := NewRecord(sk, testPath, 4, ts.Add(time.Hour*3), 0)
	require.NoError(t, err)

	assert("R1 is the only record", r1, r1)
	assert("R2 has the highest sequence number", r2, r1, r2)
	assert("R3 has the highest sequence number", r3, r1, r2, r3)
	assert("R4 has the highest EOL", r4, r1, r2, r3, r4)
	assert("R5 has the highest sequence number", r5, r1, r2, r3, r4, r5)
}

func TestValidator(t *testing.T) {
	t.Parallel()

	check := func(t *testing.T, sk ic.PrivKey, keybook peerstore.KeyBook, key, val []byte, eol time.Time, exp error, opts ...Option) {
		validator := Validator{keybook}
		data := val
		if data == nil {
			// do not call mustNewRecord because that validates the record!
			rec, err := NewRecord(sk, testPath, 1, eol, 0, opts...)
			require.NoError(t, err)
			data = mustMarshal(t, rec)
		}
		require.ErrorIs(t, validator.Validate(string(key), data), exp)
	}

	t.Run("validator returns correct errors", func(t *testing.T) {
		t.Parallel()

		ts := time.Now()
		sk, _, name := mustKeyPair(t, ic.RSA)
		sk2, _, name2 := mustKeyPair(t, ic.RSA)
		kb, err := pstoremem.NewPeerstore()
		require.NoError(t, err)
		err = kb.AddPubKey(name.Peer(), sk.GetPublic())
		require.NoError(t, err)
		emptyKB, err := pstoremem.NewPeerstore()
		require.NoError(t, err)

		check(t, sk, kb, name.RoutingKey(), nil, ts.Add(time.Hour), nil)
		check(t, sk, kb, name.RoutingKey(), nil, ts.Add(time.Hour*-1), ErrExpiredRecord)
		check(t, sk, kb, name.RoutingKey(), []byte("bad data"), ts.Add(time.Hour), ErrInvalidRecord)
		check(t, sk, kb, []byte(NamespacePrefix+"bad key"), nil, ts.Add(time.Hour), ErrInvalidName)
		check(t, sk, emptyKB, name.RoutingKey(), nil, ts.Add(time.Hour), ErrPublicKeyNotFound, WithPublicKey(false))
		check(t, sk2, kb, name2.RoutingKey(), nil, ts.Add(time.Hour), ErrPublicKeyNotFound, WithPublicKey(false))
		check(t, sk2, kb, name.RoutingKey(), nil, ts.Add(time.Hour), ErrPublicKeyMismatch)
		check(t, sk2, kb, name.RoutingKey(), nil, ts.Add(time.Hour), ErrSignature, WithPublicKey(false))
		check(t, sk, kb, []byte("//"+name.String()), nil, ts.Add(time.Hour), ErrInvalidName)
		check(t, sk, kb, []byte("/wrong/"+name.String()), nil, ts.Add(time.Hour), ErrInvalidName)
	})

	t.Run("validator uses public key", func(t *testing.T) {
		t.Parallel()

		eol := time.Now().Add(time.Hour)
		kb, err := pstoremem.NewPeerstore()
		require.NoError(t, err)

		sk, _, name := mustKeyPair(t, ic.Ed25519)
		rec := mustNewRecord(t, sk, testPath, 1, eol, 0)
		require.Empty(t, rec.pb.PubKey)
		dataNoKey := mustMarshal(t, rec)

		check(t, sk, kb, name.RoutingKey(), dataNoKey, eol, nil)
	})

	t.Run("TestEmbeddedPubKeyValidate", func(t *testing.T) {
		t.Parallel()

		eol := time.Now().Add(time.Hour)
		kb, err := pstoremem.NewPeerstore()
		require.NoError(t, err)

		sk, _, name := mustKeyPair(t, ic.RSA)
		rec := mustNewRecord(t, sk, testPath, 1, eol, 0, WithPublicKey(false))

		// Fails with RSA key without embedded public key.
		check(t, sk, kb, name.RoutingKey(), mustMarshal(t, rec), eol, ErrPublicKeyNotFound)

		// Embeds public key, must work now.
		rec = mustNewRecord(t, sk, testPath, 1, eol, 0)
		check(t, sk, kb, name.RoutingKey(), mustMarshal(t, rec), eol, nil)

		// Force bad public key. Validation fails.
		rec.pb.PubKey = []byte("probably not a public key")
		check(t, sk, kb, name.RoutingKey(), mustMarshal(t, rec), eol, ErrInvalidPublicKey)

		// Does not work with wrong key.
		sk2, _, _ := mustKeyPair(t, ic.RSA)
		wrongKey, err := ic.MarshalPublicKey(sk2.GetPublic())
		require.NoError(t, err)
		rec.pb.PubKey = wrongKey
		check(t, sk, kb, name.RoutingKey(), mustMarshal(t, rec), eol, ErrPublicKeyMismatch)
	})
}

func TestValidate(t *testing.T) {
	t.Parallel()

	t.Run("signature v1 is ignored", func(t *testing.T) {
		t.Parallel()

		eol := time.Now().Add(time.Hour)
		sk, pk, name := mustKeyPair(t, ic.Ed25519)
		ipnsRoutingKey := string(name.RoutingKey())

		v := Validator{}

		path1, err := path.Join(testPath, "1")
		require.NoError(t, err)

		path2, err := path.Join(testPath, "2")
		require.NoError(t, err)

		rec1 := mustNewRecord(t, sk, path1, 1, eol, 0, WithV1Compatibility(true))
		rec2 := mustNewRecord(t, sk, path2, 2, eol, 0, WithV1Compatibility(true))

		best, err := v.Select(ipnsRoutingKey, [][]byte{mustMarshal(t, rec1), mustMarshal(t, rec2)})
		require.NoError(t, err)
		require.Equal(t, 1, best)

		// Having only the v1 signature is invalid.
		rec2.pb.SignatureV2 = nil
		require.Error(t, Validate(rec2, pk), ErrSignature)

		// Record with v2 signature is always be preferred.
		best, err = v.Select(ipnsRoutingKey, [][]byte{mustMarshal(t, rec1), mustMarshal(t, rec2)})
		require.NoError(t, err)
		require.Equal(t, 0, best)

		// Missing v1 signature is acceptable as long as there is a valid v2 signature.
		rec1.pb.SignatureV1 = nil
		require.NoError(t, Validate(rec1, pk))

		// Invalid v1 signature is acceptable as long as there is a valid v2 signature.
		rec1.pb.SignatureV1 = []byte("garbage")
		require.NoError(t, Validate(rec1, pk))
	})

	t.Run("only signature v2 is validated", func(t *testing.T) {
		t.Parallel()

		eol := time.Now().Add(time.Hour)
		sk, pk, _ := mustKeyPair(t, ic.Ed25519)

		entry, err := NewRecord(sk, testPath, 1, eol, 0)
		require.NoError(t, err)
		require.NoError(t, Validate(entry, pk))

		entry.pb.SignatureV2 = nil
		require.ErrorIs(t, Validate(entry, pk), ErrSignature)
	})

	t.Run("max size validation", func(t *testing.T) {
		t.Parallel()

		eol := time.Now().Add(time.Hour)
		sk, pk, _ := mustKeyPair(t, ic.RSA)

		// Create a record that is too large (value + other fields).
		path, err := path.Join(testPath, string(make([]byte, MaxRecordSize)))
		require.NoError(t, err)

		rec, err := NewRecord(sk, path, 1, eol, 0)
		require.NoError(t, err)

		err = Validate(rec, pk)
		require.ErrorIs(t, err, ErrRecordSize)
	})
}

func TestValidateWithName(t *testing.T) {
	t.Parallel()

	sk, _, name := mustKeyPair(t, ic.Ed25519)
	eol := time.Now().Add(time.Hour)
	r := mustNewRecord(t, sk, testPath, 1, eol, 0)

	t.Run("valid peer ID", func(t *testing.T) {
		t.Parallel()

		err := ValidateWithName(r, name)
		assert.NoError(t, err)
	})

	t.Run("invalid peer ID", func(t *testing.T) {
		t.Parallel()

		_, _, name2 := mustKeyPair(t, ic.Ed25519)
		err := ValidateWithName(r, name2)
		assert.ErrorIs(t, err, ErrSignature)
	})
}
