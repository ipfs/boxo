package ipns

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"

	ipns_pb "github.com/ipfs/boxo/ipns/pb"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/util"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

var testPath path.Path

func init() {
	var err error
	testPath, err = path.NewPath("/ipfs/bafkqac3jobxhgidsn5rww4yk")
	if err != nil {
		panic(err)
	}
}

func mustKeyPair(t *testing.T, typ int) (ic.PrivKey, ic.PubKey, Name) {
	sk, pk, err := ic.GenerateKeyPairWithReader(typ, 2048, rand.Reader)
	require.NoError(t, err)

	pid, err := peer.IDFromPublicKey(pk)
	require.NoError(t, err)

	return sk, pk, NameFromPeer(pid)
}

func mustNewRecord(t *testing.T, sk ic.PrivKey, value path.Path, seq uint64, eol time.Time, ttl time.Duration, opts ...Option) *Record {
	rec, err := NewRecord(sk, value, seq, eol, ttl, opts...)
	require.NoError(t, err)
	require.NoError(t, Validate(rec, sk.GetPublic()))
	return rec
}

func mustNewRawRecord(t *testing.T, sk ic.PrivKey, value []byte, seq uint64, eol time.Time, ttl time.Duration, opts ...Option) *Record {
	rec, err := newRecord(sk, value, seq, eol, ttl, opts...)
	require.NoError(t, err)
	require.NoError(t, Validate(rec, sk.GetPublic()))
	return rec
}

func mustMarshal(t *testing.T, entry *Record) []byte {
	data, err := MarshalRecord(entry)
	require.NoError(t, err)
	return data
}

func fieldsMatch(t *testing.T, rec *Record, value path.Path, seq uint64, eol time.Time, ttl time.Duration) {
	recPath, err := rec.Value()
	require.NoError(t, err)
	require.Equal(t, value.String(), recPath.String())

	recSeq, err := rec.Sequence()
	require.NoError(t, err)
	require.Equal(t, seq, recSeq)

	recValidityType, err := rec.ValidityType()
	require.NoError(t, err)
	require.Equal(t, recValidityType, ValidityEOL)

	recEOL, err := rec.Validity()
	require.NoError(t, err)
	require.True(t, recEOL.Equal(eol))

	recTTL, err := rec.TTL()
	require.NoError(t, err)
	require.Equal(t, ttl, recTTL)
}

func fieldsMatchV1(t *testing.T, rec *Record, value path.Path, seq uint64, eol time.Time, ttl time.Duration) {
	require.Equal(t, value.String(), string(rec.pb.GetValue()))
	require.Equal(t, seq, rec.pb.GetSequence())
	require.Equal(t, rec.pb.GetValidityType(), ipns_pb.IpnsRecord_EOL)
	require.Equal(t, time.Duration(rec.pb.GetTtl()), ttl)

	recEOL, err := util.ParseRFC3339(string(rec.pb.GetValidity()))
	require.NoError(t, err)
	require.NoError(t, err)
	require.True(t, recEOL.Equal(eol))
}

func TestNewRecord(t *testing.T) {
	t.Parallel()

	sk, _, _ := mustKeyPair(t, ic.Ed25519)

	seq := uint64(0)
	eol := time.Now().Add(time.Hour)
	ttl := time.Minute * 10

	t.Run("V1+V2 records by default", func(t *testing.T) {
		t.Parallel()

		rec := mustNewRecord(t, sk, testPath, seq, eol, ttl)
		require.NotEmpty(t, rec.pb.SignatureV1)

		_, err := rec.PubKey()
		require.ErrorIs(t, err, ErrPublicKeyNotFound)

		fieldsMatch(t, rec, testPath, seq, eol, ttl)
		fieldsMatchV1(t, rec, testPath, seq, eol, ttl)
	})

	t.Run("V2 records with option", func(t *testing.T) {
		t.Parallel()

		rec := mustNewRecord(t, sk, testPath, seq, eol, ttl, WithV1Compatibility(false))
		require.Empty(t, rec.pb.SignatureV1)

		_, err := rec.PubKey()
		require.ErrorIs(t, err, ErrPublicKeyNotFound)

		fieldsMatch(t, rec, testPath, seq, eol, ttl)
		require.Empty(t, rec.pb.GetValue())
		require.Empty(t, rec.pb.GetSequence())
		require.Empty(t, rec.pb.GetValidity())
		require.Empty(t, rec.pb.GetValidityType())
		require.Empty(t, rec.pb.GetTtl())
	})

	t.Run("Public key embedded by default for RSA and ECDSA keys", func(t *testing.T) {
		t.Parallel()

		for _, keyType := range []int{ic.RSA, ic.ECDSA} {
			sk, _, _ := mustKeyPair(t, keyType)
			rec := mustNewRecord(t, sk, testPath, seq, eol, ttl)
			fieldsMatch(t, rec, testPath, seq, eol, ttl)

			pk, err := rec.PubKey()
			require.NoError(t, err)
			require.True(t, pk.Equals(sk.GetPublic()))
		}
	})

	t.Run("Public key not embedded by default for Ed25519 and Secp256k1 keys", func(t *testing.T) {
		t.Parallel()

		for _, keyType := range []int{ic.Ed25519, ic.Secp256k1} {
			sk, _, _ := mustKeyPair(t, keyType)
			rec := mustNewRecord(t, sk, testPath, seq, eol, ttl)
			fieldsMatch(t, rec, testPath, seq, eol, ttl)

			_, err := rec.PubKey()
			require.ErrorIs(t, err, ErrPublicKeyNotFound)
		}
	})
}

func TestExtractPublicKey(t *testing.T) {
	t.Parallel()

	t.Run("Returns expected public key when embedded in Peer ID", func(t *testing.T) {
		sk, pk, name := mustKeyPair(t, ic.Ed25519)
		rec := mustNewRecord(t, sk, testPath, 0, time.Now().Add(time.Hour), time.Minute*10, WithPublicKey(false))

		pk2, err := ExtractPublicKey(rec, name)
		require.Nil(t, err)
		require.Equal(t, pk, pk2)
	})

	t.Run("Returns expected public key when embedded in Record (by default)", func(t *testing.T) {
		sk, pk, name := mustKeyPair(t, ic.RSA)
		rec := mustNewRecord(t, sk, testPath, 0, time.Now().Add(time.Hour), time.Minute*10)

		pk2, err := ExtractPublicKey(rec, name)
		require.Nil(t, err)
		require.True(t, pk.Equals(pk2))
	})

	t.Run("Errors when not embedded in Record or Peer ID", func(t *testing.T) {
		sk, _, name := mustKeyPair(t, ic.RSA)
		rec := mustNewRecord(t, sk, testPath, 0, time.Now().Add(time.Hour), time.Minute*10, WithPublicKey(false))

		pk, err := ExtractPublicKey(rec, name)
		require.Error(t, err)
		require.Nil(t, pk)
	})

	t.Run("Errors on invalid public key bytes", func(t *testing.T) {
		sk, _, name := mustKeyPair(t, ic.Ed25519)
		rec := mustNewRecord(t, sk, testPath, 0, time.Now().Add(time.Hour), time.Minute*10)

		// Force bad pub key information.
		rec.pb.PubKey = []byte("invalid stuff")

		pk, err := ExtractPublicKey(rec, name)
		require.ErrorIs(t, err, ErrInvalidPublicKey)
		require.Nil(t, pk)
	})
}

func TestCBORDataSerialization(t *testing.T) {
	t.Parallel()

	sk, _, _ := mustKeyPair(t, ic.Ed25519)

	eol := time.Now().Add(time.Hour)
	path, err := path.Join(testPath, string([]byte{0x00}))
	require.NoError(t, err)
	seq := uint64(1)
	ttl := time.Hour

	rec := mustNewRecord(t, sk, path, seq, eol, ttl)

	builder := basicnode.Prototype__Map{}.NewBuilder()
	err = dagcbor.Decode(builder, bytes.NewReader(rec.pb.GetData()))
	require.NoError(t, err)

	node := builder.Build()
	iter := node.MapIterator()
	var fields []string
	for !iter.Done() {
		k, v, err := iter.Next()
		require.NoError(t, err)
		kStr, err := k.AsString()
		require.NoError(t, err)

		switch kStr {
		case cborValueKey:
			b, err := v.AsBytes()
			require.NoError(t, err)
			require.Equal(t, b, []byte(path.String()))
		case cborSequenceKey:
			s, err := v.AsInt()
			require.NoError(t, err)
			require.Equal(t, seq, uint64(s))
		case cborValidityKey:
			val, err := v.AsBytes()
			require.NoError(t, err)
			require.Equal(t, []byte(util.FormatRFC3339(eol)), val)
		case cborValidityTypeKey:
			vt, err := v.AsInt()
			require.NoError(t, err)
			require.Equal(t, uint64(0), uint64(vt))
		case cborTTLKey:
			ttlVal, err := v.AsInt()
			require.NoError(t, err)
			require.Equal(t, ttl, time.Duration(ttlVal))
		}

		fields = append(fields, kStr)
	}

	// Ensure correct key order, i.e., by length then value.
	expectedOrder := []string{"TTL", "Value", "Sequence", "Validity", "ValidityType"}
	require.Len(t, fields, len(expectedOrder))
	for i, f := range fields {
		expected := expectedOrder[i]
		assert.Equal(t, expected, f)
	}
}

func TestUnmarshal(t *testing.T) {
	t.Parallel()

	sk, _, _ := mustKeyPair(t, ic.Ed25519)

	seq := uint64(0)
	eol := time.Now().Add(time.Hour)
	ttl := time.Minute * 10

	t.Run("Errors on invalid bytes", func(t *testing.T) {
		_, err := UnmarshalRecord([]byte("blah blah blah"))
		require.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("Errors if record is too long", func(t *testing.T) {
		data := make([]byte, MaxRecordSize+1)
		_, err := UnmarshalRecord(data)
		require.ErrorIs(t, err, ErrRecordSize)
	})

	t.Run("Errors with V1-only records", func(t *testing.T) {
		pb := ipns_pb.IpnsRecord{}
		data, err := proto.Marshal(&pb)
		require.NoError(t, err)
		_, err = UnmarshalRecord(data)
		require.ErrorIs(t, err, ErrDataMissing)
	})

	t.Run("Errors on bad data", func(t *testing.T) {
		pb := ipns_pb.IpnsRecord{
			Data: []byte("definitely not cbor"),
		}
		data, err := proto.Marshal(&pb)
		require.NoError(t, err)
		_, err = UnmarshalRecord(data)
		require.ErrorIs(t, err, ErrInvalidRecord)
	})

	t.Run("Errors on bad binary Value() unable to represent as a Path (interop)", func(t *testing.T) {
		t.Parallel()

		// create record with non-empty byte value that is not []byte CID nor a string with /ipfs/cid content path
		rawByteValue := []byte{0x4A, 0x1B, 0x3C, 0x8D, 0x2E}
		rec := mustNewRawRecord(t, sk, rawByteValue, seq, eol, ttl)

		// confirm raw record has same bytes
		require.Equal(t, rawByteValue, rec.pb.GetValue())
		cborValue, err := rec.getBytesValue(cborValueKey)
		require.NoError(t, err)
		require.Equal(t, rawByteValue, cborValue)

		// confirm rec.Value() returns error
		recPath, err := rec.Value()
		require.ErrorIs(t, err, ErrInvalidPath)
		require.Empty(t, recPath)
	})

	t.Run("Reads record with empty Value() as zero-length identity CID (interop)", func(t *testing.T) {
		t.Parallel()

		// create record with empty byte value
		rawByteValue := []byte{}
		rec := mustNewRawRecord(t, sk, rawByteValue, seq, eol, ttl)

		// confirm raw record has empty (0-length []byte) Value
		require.Empty(t, rec.pb.GetValue())
		cborValue, err := rec.getBytesValue(cborValueKey)
		require.NoError(t, err)
		require.Empty(t, cborValue)

		// confirm rec.Value() returns NooPath for interop
		recPath, err := rec.Value()
		require.NoError(t, err)
		require.Equal(t, NoopValue, recPath.String())
	})

	t.Run("Reads record with a []byte CID as Value() (interop)", func(t *testing.T) {
		t.Parallel()

		// create record with a valid CID in binary form
		// instead of /ipfs/cid string
		// (we need to support this because this was allowed since <2018)
		testCid := "bafkqablimvwgy3y"
		rawCidValue := cid.MustParse(testCid).Bytes()

		rec := mustNewRawRecord(t, sk, rawCidValue, seq, eol, ttl)

		// confirm raw record has same bytes
		require.Equal(t, rawCidValue, rec.pb.GetValue())
		cborValue, err := rec.getBytesValue(cborValueKey)
		require.NoError(t, err)
		require.Equal(t, rawCidValue, cborValue)

		// confirm rec.Value() returns path for raw CID
		recPath, err := rec.Value()
		require.NoError(t, err)
		require.Equal(t, "/ipfs/"+testCid, recPath.String())
	})
}
