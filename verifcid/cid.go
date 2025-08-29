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
	// MaxDigestSize is the maximum size for all digest types (including identity)
	MaxDigestSize = 128
)

var (
	ErrPossiblyInsecureHashFunction = errors.New("potentially insecure hash functions not allowed")
	ErrDigestTooSmall               = fmt.Errorf("digest too small: must be at least %d bytes", MinDigestSize)
	ErrDigestTooLarge               = fmt.Errorf("digest too large: must be at most %d bytes", MaxDigestSize)
	ErrIdentityDigestTooLarge       = fmt.Errorf("identity digest too large: must be at most %d bytes", MaxDigestSize)

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

	if pref.MhType != mh.IDENTITY && pref.MhLength < MinDigestSize {
		return ErrDigestTooSmall
	}

	if pref.MhLength > MaxDigestSize {
		if pref.MhType == mh.IDENTITY {
			return ErrIdentityDigestTooLarge
		}
		return ErrDigestTooLarge
	}

	return nil
}
