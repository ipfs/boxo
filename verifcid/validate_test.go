package verifcid

import (
	"testing"

	mh "github.com/multiformats/go-multihash"

	cid "github.com/ipfs/go-cid"
)

func TestValidateCids(t *testing.T) {
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

	assertTrue(IsGoodHash(mh.SHA2_256))
	assertTrue(IsGoodHash(mh.BLAKE2B_MIN + 32))
	assertTrue(IsGoodHash(mh.DBL_SHA2_256))
	assertTrue(IsGoodHash(mh.KECCAK_256))
	assertTrue(IsGoodHash(mh.SHA3))

	assertTrue(IsGoodHash(mh.SHA1))

	assertFalse(IsGoodHash(mh.BLAKE2B_MIN + 5))

	mhcid := func(code uint64, length int) cid.Cid {
		mhash, err := mh.Sum([]byte{}, code, length)
		if err != nil {
			t.Fatalf("%v: code: %x length: %d", err, code, length)
		}
		return cid.NewCidV1(cid.DagCBOR, mhash)
	}

	cases := []struct {
		cid cid.Cid
		err error
	}{
		{mhcid(mh.SHA2_256, 32), nil},
		{mhcid(mh.SHA2_256, 16), ErrBelowMinimumHashLength},
		{mhcid(mh.MURMUR3X64_64, 4), ErrPossiblyInsecureHashFunction},
		{mhcid(mh.BLAKE3, 32), nil},
		{mhcid(mh.BLAKE3, 69), nil},
		{mhcid(mh.BLAKE3, 128), nil},
	}

	for i, cas := range cases {
		if ValidateCid(cas.cid) != cas.err {
			t.Errorf("wrong result in case of %s (index %d). Expected: %s, got %s",
				cas.cid, i, cas.err, ValidateCid(cas.cid))
		}
	}

	longBlake3Hex := "1e810104e0bb39f30b1a3feb89f536c93be15055482df748674b00d26e5a75777702e9791074b7511b59d31c71c62f5a745689fa6c9497f68bdf1061fe07f518d410c0b0c27f41b3cf083f8a7fdc67a877e21790515762a754a45dcb8a356722698a7af5ed2bb608983d5aa75d4d61691ef132efe8631ce0afc15553a08fffc60ee9369b"
	longBlake3Mh, err := mh.FromHexString(longBlake3Hex)
	if err != nil {
		t.Fatalf("failed to produce a multihash from the long blake3 hash: %v", err)
	}
	if ValidateCid(cid.NewCidV1(cid.DagCBOR, longBlake3Mh)) != ErrAboveMaximumHashLength {
		t.Errorf("a CID that was longer than the maximum hash length did not error with ErrAboveMaximumHashLength")
	}
}
