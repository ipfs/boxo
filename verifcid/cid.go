package verifcid

import (
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

const (
	// MinDigestSize is the minimum size for hash digests (except for identity hashes)
	MinDigestSize = 20
	// MaxDigestSize is the maximum size for cryptographic hash digests
	MaxDigestSize = 128
	// MaxIdentityDigestSize is the maximum size for identity CID digests
	// Identity CIDs embed data directly, and this limit prevents abuse
	MaxIdentityDigestSize = 128
)

var (
	ErrPossiblyInsecureHashFunction = errors.New("potentially insecure hash functions not allowed")
	ErrDigestTooSmall               = errors.New("digest too small")
	ErrDigestTooLarge               = errors.New("digest too large")
	ErrIdentityDigestTooLarge       = errors.New("identity digest too large")

	// Deprecated: Use ErrDigestTooSmall instead
	ErrBelowMinimumHashLength = ErrDigestTooSmall
	// Deprecated: Use ErrDigestTooLarge instead
	ErrAboveMaximumHashLength = ErrDigestTooLarge
)

// ValidateCid validates multihash allowance behind given CID.
func ValidateCid(allowlist Allowlist, c cid.Cid) error {
	pref := c.Prefix()
	if !allowlist.IsAllowed(pref.MhType) {
		return ErrPossiblyInsecureHashFunction
	}

	switch pref.MhType {
	case mh.IDENTITY:
		if pref.MhLength > MaxIdentityDigestSize {
			return newErrIdentityDigestTooLarge(pref.MhLength)
		}
	default:
		if pref.MhLength < MinDigestSize {
			return newErrDigestTooSmall(pref.MhLength)
		}
		if pref.MhLength > MaxDigestSize {
			return newErrDigestTooLarge(pref.MhLength)
		}
	}

	return nil
}

func newErrDigestTooSmall(got int) error {
	return fmt.Errorf("%w: got %d bytes, minimum %d", ErrDigestTooSmall, got, MinDigestSize)
}

func newErrDigestTooLarge(got int) error {
	return fmt.Errorf("%w: got %d bytes, maximum %d", ErrDigestTooLarge, got, MaxDigestSize)
}

func newErrIdentityDigestTooLarge(got int) error {
	return fmt.Errorf("%w: got %d bytes, maximum %d", ErrIdentityDigestTooLarge, got, MaxIdentityDigestSize)
}
