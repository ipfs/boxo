package ipns

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	pb "github.com/ipfs/go-ipns/pb"

	ipldcodec "github.com/ipld/go-ipld-prime/multicodec"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/multiformats/go-multicodec"

	proto "github.com/gogo/protobuf/proto"
	u "github.com/ipfs/go-ipfs-util"
	"github.com/libp2p/go-libp2p-core/crypto"
	peer "github.com/libp2p/go-libp2p-core/peer"
	pstore "github.com/libp2p/go-libp2p-core/peerstore"
	pstoremem "github.com/libp2p/go-libp2p-peerstore/pstoremem"
)

func testValidatorCase(t *testing.T, priv crypto.PrivKey, kbook pstore.KeyBook, key string, val []byte, eol time.Time, exp error) {
	t.Helper()

	match := func(t *testing.T, err error) {
		t.Helper()
		if err != exp {
			params := fmt.Sprintf("key: %s\neol: %s\n", key, eol)
			if exp == nil {
				t.Fatalf("Unexpected error %s for params %s", err, params)
			} else if err == nil {
				t.Fatalf("Expected error %s but there was no error for params %s", exp, params)
			} else {
				t.Fatalf("Expected error %s but got %s for params %s", exp, err, params)
			}
		}
	}

	testValidatorCaseMatchFunc(t, priv, kbook, key, val, eol, match)
}

func testValidatorCaseMatchFunc(t *testing.T, priv crypto.PrivKey, kbook pstore.KeyBook, key string, val []byte, eol time.Time, matchf func(*testing.T, error)) {
	t.Helper()
	validator := Validator{kbook}

	data := val
	if data == nil {
		p := []byte("/ipfs/QmfM2r8seH2GiRaC4esTjeraXEachRt8ZsSeGaWTPLyMoG")
		entry, err := Create(priv, p, 1, eol, 0)
		if err != nil {
			t.Fatal(err)
		}

		data, err = proto.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
	}

	matchf(t, validator.Validate(key, data))
}

func TestValidator(t *testing.T) {
	ts := time.Now()

	priv, id, _ := genKeys(t)
	priv2, id2, _ := genKeys(t)
	kbook := pstoremem.NewPeerstore()
	if err := kbook.AddPubKey(id, priv.GetPublic()); err != nil {
		t.Fatal(err)
	}
	emptyKbook := pstoremem.NewPeerstore()

	testValidatorCase(t, priv, kbook, "/ipns/"+string(id), nil, ts.Add(time.Hour), nil)
	testValidatorCase(t, priv, kbook, "/ipns/"+string(id), nil, ts.Add(time.Hour*-1), ErrExpiredRecord)
	testValidatorCase(t, priv, kbook, "/ipns/"+string(id), []byte("bad data"), ts.Add(time.Hour), ErrBadRecord)
	testValidatorCase(t, priv, kbook, "/ipns/"+"bad key", nil, ts.Add(time.Hour), ErrKeyFormat)
	testValidatorCase(t, priv, emptyKbook, "/ipns/"+string(id), nil, ts.Add(time.Hour), ErrPublicKeyNotFound)
	testValidatorCase(t, priv2, kbook, "/ipns/"+string(id2), nil, ts.Add(time.Hour), ErrPublicKeyNotFound)
	testValidatorCase(t, priv2, kbook, "/ipns/"+string(id), nil, ts.Add(time.Hour), ErrSignature)
	testValidatorCase(t, priv, kbook, "//"+string(id), nil, ts.Add(time.Hour), ErrInvalidPath)
	testValidatorCase(t, priv, kbook, "/wrong/"+string(id), nil, ts.Add(time.Hour), ErrInvalidPath)
}

func mustMarshal(t *testing.T, entry *pb.IpnsEntry) []byte {
	t.Helper()
	data, err := proto.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestEmbeddedPubKeyValidate(t *testing.T) {
	goodeol := time.Now().Add(time.Hour)
	kbook := pstoremem.NewPeerstore()

	pth := []byte("/ipfs/QmfM2r8seH2GiRaC4esTjeraXEachRt8ZsSeGaWTPLyMoG")

	priv, _, ipnsk := genKeys(t)

	entry, err := Create(priv, pth, 1, goodeol, 0)
	if err != nil {
		t.Fatal(err)
	}

	testValidatorCase(t, priv, kbook, ipnsk, mustMarshal(t, entry), goodeol, ErrPublicKeyNotFound)

	pubkb, err := crypto.MarshalPublicKey(priv.GetPublic())
	if err != nil {
		t.Fatal(err)
	}

	entry.PubKey = pubkb
	testValidatorCase(t, priv, kbook, ipnsk, mustMarshal(t, entry), goodeol, nil)

	entry.PubKey = []byte("probably not a public key")
	testValidatorCaseMatchFunc(t, priv, kbook, ipnsk, mustMarshal(t, entry), goodeol, func(t *testing.T, err error) {
		if !strings.Contains(err.Error(), "unmarshaling pubkey in record:") {
			t.Fatal("expected pubkey unmarshaling to fail")
		}
	})

	opriv, _, _ := genKeys(t)
	wrongkeydata, err := crypto.MarshalPublicKey(opriv.GetPublic())
	if err != nil {
		t.Fatal(err)
	}

	entry.PubKey = wrongkeydata
	testValidatorCase(t, priv, kbook, ipnsk, mustMarshal(t, entry), goodeol, ErrPublicKeyMismatch)
}

func TestPeerIDPubKeyValidate(t *testing.T) {
	t.Skip("disabled until libp2p/go-libp2p-crypto#51 is fixed")

	goodeol := time.Now().Add(time.Hour)
	kbook := pstoremem.NewPeerstore()

	pth := []byte("/ipfs/QmfM2r8seH2GiRaC4esTjeraXEachRt8ZsSeGaWTPLyMoG")

	sk, pk, err := crypto.GenerateEd25519Key(rand.New(rand.NewSource(42)))
	if err != nil {
		t.Fatal(err)
	}

	pid, err := peer.IDFromPublicKey(pk)
	if err != nil {
		t.Fatal(err)
	}

	ipnsk := "/ipns/" + string(pid)

	entry, err := Create(sk, pth, 1, goodeol, 0)
	if err != nil {
		t.Fatal(err)
	}

	dataNoKey, err := proto.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}

	testValidatorCase(t, sk, kbook, ipnsk, dataNoKey, goodeol, nil)
}

func TestBothSignatureVersionsValidate(t *testing.T) {
	goodeol := time.Now().Add(time.Hour)

	sk, pk, err := crypto.GenerateEd25519Key(rand.New(rand.NewSource(42)))
	if err != nil {
		t.Fatal(err)
	}

	path1 := []byte("/path/1")
	entry, err := Create(sk, path1, 1, goodeol, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := Validate(pk, entry); err != nil {
		t.Fatal(err)
	}

	entry.SignatureV2 = nil
	if err := Validate(pk, entry); err != nil {
		t.Fatal(err)
	}

	entry.SignatureV1 = nil
	if err := Validate(pk, entry); !errors.Is(err, ErrSignature) {
		t.Fatal(err)
	}
}

func TestNewSignatureVersionPreferred(t *testing.T) {
	goodeol := time.Now().Add(time.Hour)

	sk, pk, err := crypto.GenerateEd25519Key(rand.New(rand.NewSource(42)))
	if err != nil {
		t.Fatal(err)
	}

	pid, err := peer.IDFromPublicKey(pk)
	if err != nil {
		t.Fatal(err)
	}

	ipnsk := "/ipns/" + string(pid)

	path1 := []byte("/path/1")
	entry1, err := Create(sk, path1, 1, goodeol, 0)
	if err != nil {
		t.Fatal(err)
	}

	path2 := []byte("/path/2")
	entry2, err := Create(sk, path2, 2, goodeol, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := Validate(pk, entry1); err != nil {
		t.Fatal(err)
	}

	if err := Validate(pk, entry2); err != nil {
		t.Fatal(err)
	}

	v := Validator{}
	best, err := v.Select(ipnsk, [][]byte{mustMarshal(t, entry1), mustMarshal(t, entry2)})
	if err != nil {
		t.Fatal(err)
	}
	if best != 1 {
		t.Fatal("entry2 should be better than entry1")
	}

	// Having only the v1 signature should be valid
	entry2.SignatureV2 = nil
	if err := Validate(pk, entry2); err != nil {
		t.Fatal(err)
	}

	// However the v2 signature should be preferred
	best, err = v.Select(ipnsk, [][]byte{mustMarshal(t, entry1), mustMarshal(t, entry2)})
	if err != nil {
		t.Fatal(err)
	}
	if best != 0 {
		t.Fatal("entry1 should be better than entry2")
	}

	// Having a missing v1 signature is acceptable as long as there is a valid v2 signature
	entry1.SignatureV1 = nil
	if err := Validate(pk, entry1); err != nil {
		t.Fatal(err)
	}

	// Having an invalid v1 signature is acceptable as long as there is a valid v2 signature
	entry1.SignatureV1 = []byte("garbage")
	if err := Validate(pk, entry1); err != nil {
		t.Fatal(err)
	}
}

func TestCborDataCanonicalization(t *testing.T) {
	goodeol := time.Now().Add(time.Hour)

	sk, pk, err := crypto.GenerateEd25519Key(rand.New(rand.NewSource(42)))
	if err != nil {
		t.Fatal(err)
	}

	path := append([]byte("/path/1"), 0x00)
	seqnum := uint64(1)
	entry, err := Create(sk, path, seqnum, goodeol, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	if err := Validate(pk, entry); err != nil {
		t.Fatal(err)
	}

	dec, err := ipldcodec.LookupDecoder(uint64(multicodec.DagCbor))
	if err != nil {
		t.Fatal(err)
	}

	ndbuilder := basicnode.Prototype__Map{}.NewBuilder()
	if err := dec(ndbuilder, bytes.NewReader(entry.GetData())); err != nil {
		t.Fatal(err)
	}

	nd := ndbuilder.Build()
	iter := nd.MapIterator()
	var fields []string
	for !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		kstr, err := k.AsString()
		if err != nil {
			t.Fatal(err)
		}

		switch kstr {
		case value:
			b, err := v.AsBytes()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(path, b) {
				t.Fatal("value did not match")
			}
		case sequence:
			s, err := v.AsInt()
			if err != nil {
				t.Fatal(err)
			}
			if uint64(s) != seqnum {
				t.Fatal("sequence numbers did not match")
			}
		case validity:
			val, err := v.AsBytes()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(val, []byte(u.FormatRFC3339(goodeol))) {
				t.Fatal("validity did not match")
			}
		case validityType:
			vt, err := v.AsInt()
			if err != nil {
				t.Fatal(err)
			}
			if uint64(vt) != 0 {
				t.Fatal("validity types did not match")
			}
		case ttl:
			ttlVal, err := v.AsInt()
			if err != nil {
				t.Fatal(err)
			}
			// TODO: test non-zero TTL
			if uint64(ttlVal) != uint64(time.Hour.Nanoseconds()) {
				t.Fatal("TTLs did not match")
			}
		}

		fields = append(fields, kstr)
	}

	// Check for map sort order (i.e. by length then by value)
	expectedOrder := []string{"TTL", "Value", "Sequence", "Validity", "ValidityType"}
	if len(fields) != len(expectedOrder) {
		t.Fatal("wrong number of fields")
	}

	for i, f := range fields {
		expected := expectedOrder[i]
		if f != expected {
			t.Fatalf("expected %s, got %s", expected, f)
		}
	}
}

func genKeys(t *testing.T) (crypto.PrivKey, peer.ID, string) {
	sr := u.NewTimeSeededRand()
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, sr)
	if err != nil {
		t.Fatal(err)
	}

	// Create entry with expiry in one hour
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	ipnsKey := RecordKey(pid)

	return priv, pid, ipnsKey
}
