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
	ErrBelowMinDigestSize           = fmt.Errorf("multihash digest must be at least %d bytes long", MinDigestSize)
	ErrAboveMaxDigestSize           = fmt.Errorf("multihash digest must be at most %d bytes long", MaxDigestSize)

	// Deprecated: Use ErrBelowMinDigestSize instead
	ErrBelowMinimumHashLength = ErrBelowMinDigestSize
	// Deprecated: Use ErrAboveMaxDigestSize instead
	ErrAboveMaximumHashLength = ErrAboveMaxDigestSize
)

// ValidateCid validates multihash allowance behind given CID.
func ValidateCid(allowlist Allowlist, c cid.Cid) error {
	pref := c.Prefix()
	if !allowlist.IsAllowed(pref.MhType) {
		return ErrPossiblyInsecureHashFunction
	}

	if pref.MhType != mh.IDENTITY && pref.MhLength < MinDigestSize {
		return ErrBelowMinDigestSize
	}

	if pref.MhLength > MaxDigestSize {
		return ErrAboveMaxDigestSize
	}

	return nil
}
