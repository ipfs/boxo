package contentrouter

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
)

// peerIDFromDIDKey attempts to derive a libp2p PeerID from a did:key: identifier.
// Currently supports ed25519 public keys (multicodec 0xed).
func peerIDFromDIDKey(id string) (peer.ID, error) {
	if !strings.HasPrefix(id, "did:key:") {
		return "", fmt.Errorf("not a did:key identifier")
	}
	encoded := strings.TrimPrefix(id, "did:key:")

	// Decode the multibase-encoded part (z prefix = base58btc)
	_, data, err := multibase.Decode(encoded)
	if err != nil {
		return "", fmt.Errorf("multibase decode: %w", err)
	}

	// Read the multicodec varint
	codec, n, err := varint.FromUvarint(data)
	if err != nil {
		return "", fmt.Errorf("varint decode: %w", err)
	}

	// Only handle ed25519-pub (0xed) for now
	if multicodec.Code(codec) != multicodec.Ed25519Pub {
		return "", fmt.Errorf("unsupported key type: 0x%x", codec)
	}

	keyBytes := data[n:]
	pubKey, err := crypto.UnmarshalEd25519PublicKey(keyBytes)
	if err != nil {
		return "", fmt.Errorf("unmarshal ed25519: %w", err)
	}

	return peer.IDFromPublicKey(pubKey)
}

// peerIDPlaceholderNonce is a random value generated once per process,
// mixed into placeholder PeerID hashes to ensure different processes
// produce different placeholders for the same provider ID.
// TODO: make this configurable or find a better way to ensure uniqueness
// across processes while keeping determinism within one process.
var peerIDPlaceholderNonce = func() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}()

// peerIDPlaceholderFromArbitraryID generates a placeholder PeerID by hashing
// the given identifier with a per-process nonce and domain-specific salt.
//
// The resulting PeerID is deterministic within a single process (same input
// always produces the same PeerID) but differs across processes. This is a
// compatibility stub for providers that use identifiers which are not
// parseable as PeerIDs or did:key: values. The resulting PeerID is only
// used to pass addresses through legacy libp2p routing APIs (like Kubo and
// Rainbow) that require a PeerID. It does not represent a real libp2p
// identity: no valid private key exists for this PeerID because it is a
// SHA-256 multihash of arbitrary data, not a key-derived identity.
func peerIDPlaceholderFromArbitraryID(id string) peer.ID {
	mh, _ := multihash.Sum([]byte("contentrouter/peerIDPlaceholder:"+peerIDPlaceholderNonce+":"+id), multihash.SHA2_256, -1)
	return peer.ID(mh)
}

// hasHTTPURL returns true if any address is an http:// or https:// URL.
func hasHTTPURL(addrs types.Addresses) bool {
	for i := range addrs {
		if u := addrs[i].URL(); u != nil {
			s := strings.ToLower(u.Scheme)
			if s == "http" || s == "https" {
				return true
			}
		}
	}
	return false
}
