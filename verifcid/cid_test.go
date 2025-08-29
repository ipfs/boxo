package verifcid

import (
	"bytes"
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

func TestValidateCid(t *testing.T) {
	allowlist := DefaultAllowlist

	tests := []struct {
		name    string
		data    []byte
		mhType  uint64
		wantErr error
	}{
		{
			name:    "identity at max size",
			data:    bytes.Repeat([]byte("a"), MaxDigestSize),
			mhType:  mh.IDENTITY,
			wantErr: nil,
		},
		{
			name:    "identity over max size",
			data:    bytes.Repeat([]byte("b"), MaxDigestSize+1),
			mhType:  mh.IDENTITY,
			wantErr: ErrIdentityDigestTooLarge,
		},
		{
			name:    "identity at 64 bytes",
			data:    bytes.Repeat([]byte("c"), 64),
			mhType:  mh.IDENTITY,
			wantErr: nil,
		},
		{
			name:    "identity with 1KB data",
			data:    bytes.Repeat([]byte("d"), 1024),
			mhType:  mh.IDENTITY,
			wantErr: ErrIdentityDigestTooLarge,
		},
		{
			name:    "small identity",
			data:    []byte("hello"),
			mhType:  mh.IDENTITY,
			wantErr: nil,
		},
		{
			name:    "empty identity",
			data:    []byte{},
			mhType:  mh.IDENTITY,
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash, err := mh.Sum(tt.data, tt.mhType, -1)
			if err != nil {
				t.Fatal(err)
			}
			c := cid.NewCidV1(cid.Raw, hash)

			err = ValidateCid(allowlist, c)
			if err != tt.wantErr {
				t.Errorf("ValidateCid() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	t.Run("identity hash extracts correct data", func(t *testing.T) {
		// Verify we can extract the original data from identity CID
		originalData := []byte("inline data in CID")
		hash, err := mh.Sum(originalData, mh.IDENTITY, -1)
		if err != nil {
			t.Fatal(err)
		}
		c := cid.NewCidV1(cid.Raw, hash)

		// Validate the CID
		err = ValidateCid(allowlist, c)
		if err != nil {
			t.Errorf("expected valid identity CID, got error: %v", err)
		}

		// Extract and verify the data
		decoded, err := mh.Decode(c.Hash())
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Code != mh.IDENTITY {
			t.Errorf("expected identity hash code, got: %v", decoded.Code)
		}
		if !bytes.Equal(decoded.Digest, originalData) {
			t.Errorf("extracted data doesn't match original")
		}
	})

	t.Run("regular hash still respects minimum", func(t *testing.T) {
		// Create a SHA256 hash with less than MinDigestSize bytes (truncated)
		data := []byte("test data")
		fullHash, err := mh.Sum(data, mh.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}

		// Manually create a truncated hash (19 bytes)
		decoded, err := mh.Decode(fullHash)
		if err != nil {
			t.Fatal(err)
		}
		truncatedDigest := decoded.Digest[:MinDigestSize-1]
		truncatedHash, err := mh.Encode(truncatedDigest, mh.SHA2_256)
		if err != nil {
			t.Fatal(err)
		}

		c := cid.NewCidV1(cid.Raw, truncatedHash)

		err = ValidateCid(allowlist, c)
		if err != ErrDigestTooSmall {
			t.Errorf("expected ErrDigestTooSmall for truncated SHA256, got: %v", err)
		}
	})

	t.Run("identity hash exempt from minimum size", func(t *testing.T) {
		// Test that identity hashes below MinDigestSize are still valid
		// This confirms identity hashes are exempt from the minimum size requirement
		smallData := []byte("tiny") // Only 4 bytes, well below MinDigestSize of 20
		hash, err := mh.Sum(smallData, mh.IDENTITY, -1)
		if err != nil {
			t.Fatal(err)
		}
		c := cid.NewCidV1(cid.Raw, hash)

		// Should pass validation despite being below MinDigestSize
		err = ValidateCid(allowlist, c)
		if err != nil {
			t.Errorf("identity CID with %d bytes should be valid (exempt from minimum), got error: %v", len(smallData), err)
		}

		// Also test with exactly MinDigestSize-1 bytes for clarity
		dataAt19Bytes := bytes.Repeat([]byte("x"), MinDigestSize-1)
		hash19, err := mh.Sum(dataAt19Bytes, mh.IDENTITY, -1)
		if err != nil {
			t.Fatal(err)
		}
		c19 := cid.NewCidV1(cid.Raw, hash19)

		err = ValidateCid(allowlist, c19)
		if err != nil {
			t.Errorf("identity CID with %d bytes should be valid (exempt from minimum), got error: %v", MinDigestSize-1, err)
		}
	})
}
