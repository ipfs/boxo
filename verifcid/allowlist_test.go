package verifcid

import (
	"errors"
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

func TestDefaultAllowList(t *testing.T) {
	assertTrue := func(v bool) {
		t.Helper()
		if !v {
			t.Fatal("expected success")
		}
	}
	assertFalse := func(v bool) {
		t.Helper()
		if v {
			t.Fatal("expected failure")
		}
	}

	allowlist := DefaultAllowlist
	assertTrue(allowlist.IsAllowed(mh.SHA2_256))
	assertTrue(allowlist.IsAllowed(mh.BLAKE2B_MIN + 32))
	assertTrue(allowlist.IsAllowed(mh.DBL_SHA2_256))
	assertTrue(allowlist.IsAllowed(mh.KECCAK_256))
	assertTrue(allowlist.IsAllowed(mh.SHA3))
	assertTrue(allowlist.IsAllowed(mh.SHA1))
	assertTrue(allowlist.IsAllowed(mh.IDENTITY))
	assertFalse(allowlist.IsAllowed(mh.BLAKE2B_MIN + 5))

	cases := []struct {
		cid cid.Cid
		err error
	}{
		{mhcid(t, mh.SHA2_256, 32), nil},
		{mhcid(t, mh.SHA2_256, 16), ErrDigestTooSmall},
		{mhcid(t, mh.MURMUR3X64_64, 4), ErrPossiblyInsecureHashFunction},
		{mhcid(t, mh.BLAKE3, 32), nil},
		{mhcid(t, mh.BLAKE3, 69), nil},
		{mhcid(t, mh.BLAKE3, 128), nil},
		{identityCid(t, 19), nil},                        // identity below MinDigestSize (exempt from minimum)
		{identityCid(t, 64), nil},                        // identity under MaxIdentityDigestSize
		{identityCid(t, 128), nil},                       // identity at MaxIdentityDigestSize
		{identityCid(t, 129), ErrIdentityDigestTooLarge}, // identity above MaxIdentityDigestSize
	}

	for i, cas := range cases {
		err := ValidateCid(allowlist, cas.cid)
		if cas.err == nil {
			if err != nil {
				t.Errorf("wrong result in case of %s (index %d). Expected: nil, got %s",
					cas.cid, i, err)
			}
		} else if !errors.Is(err, cas.err) {
			t.Errorf("wrong result in case of %s (index %d). Expected: %s, got %s",
				cas.cid, i, cas.err, err)
		}
	}

	longBlake3Hex := "1e810104e0bb39f30b1a3feb89f536c93be15055482df748674b00d26e5a75777702e9791074b7511b59d31c71c62f5a745689fa6c9497f68bdf1061fe07f518d410c0b0c27f41b3cf083f8a7fdc67a877e21790515762a754a45dcb8a356722698a7af5ed2bb608983d5aa75d4d61691ef132efe8631ce0afc15553a08fffc60ee9369b"
	longBlake3Mh, err := mh.FromHexString(longBlake3Hex)
	if err != nil {
		t.Fatalf("failed to produce a multihash from the long blake3 hash: %v", err)
	}
	if err := ValidateCid(allowlist, cid.NewCidV1(cid.DagCBOR, longBlake3Mh)); !errors.Is(err, ErrDigestTooLarge) {
		t.Errorf("a CID that was longer than the maximum hash length did not error with ErrDigestTooLarge, got: %v", err)
	}
}

// mhcid creates a CID with the specified multihash type and length
func mhcid(t *testing.T, code uint64, length int) cid.Cid {
	t.Helper()
	mhash, err := mh.Sum([]byte{}, code, length)
	if err != nil {
		t.Fatalf("%v: code: %x length: %d", err, code, length)
	}
	return cid.NewCidV1(cid.DagCBOR, mhash)
}

// identityCid creates an identity CID with specific data size
func identityCid(t *testing.T, size int) cid.Cid {
	t.Helper()
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	hash, err := mh.Sum(data, mh.IDENTITY, -1)
	if err != nil {
		t.Fatalf("failed to create identity hash: %v", err)
	}
	return cid.NewCidV1(cid.Raw, hash)
}
