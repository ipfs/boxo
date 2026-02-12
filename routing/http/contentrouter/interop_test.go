package contentrouter

import (
	"crypto/rand"
	"testing"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasHTTPURL(t *testing.T) {
	tests := []struct {
		name   string
		addrs  []string
		expect bool
	}{
		{
			name:   "http URL",
			addrs:  []string{"http://example.com"},
			expect: true,
		},
		{
			name:   "https URL",
			addrs:  []string{"https://example.com"},
			expect: true,
		},
		{
			name:   "non-HTTP URL",
			addrs:  []string{"foo://example.com"},
			expect: false,
		},
		{
			name:   "multiaddr only",
			addrs:  []string{"/ip4/127.0.0.1/tcp/4001"},
			expect: false,
		},
		{
			name:   "mixed with one https",
			addrs:  []string{"foo://example.com", "https://example.com"},
			expect: true,
		},
		{
			name:   "empty slice",
			addrs:  nil,
			expect: false,
		},
		{
			name:   "single invalid address",
			addrs:  []string{"not-a-valid-address"},
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var addrs types.Addresses
			for _, s := range tt.addrs {
				a, err := types.NewAddress(s)
				if err != nil {
					// invalid addresses are stored with nil url/multiaddr
					addrs = append(addrs, types.Address{})
					continue
				}
				addrs = append(addrs, a)
			}
			assert.Equal(t, tt.expect, hasHTTPURL(addrs))
		})
	}
}

// TestPeerIDPlaceholderFromArbitraryID tests the determinism and uniqueness
// properties of peerIDPlaceholderFromArbitraryID directly. Within a single
// process, the same input must always produce the same PeerID, and different
// inputs must produce different PeerIDs.
func TestPeerIDPlaceholderFromArbitraryID(t *testing.T) {
	t.Run("deterministic within process", func(t *testing.T) {
		a := peerIDPlaceholderFromArbitraryID("provider-A")
		b := peerIDPlaceholderFromArbitraryID("provider-A")
		assert.Equal(t, a, b)
	})

	t.Run("different inputs produce different PeerIDs", func(t *testing.T) {
		a := peerIDPlaceholderFromArbitraryID("provider-A")
		b := peerIDPlaceholderFromArbitraryID("provider-B")
		assert.NotEqual(t, a, b)
	})

	t.Run("result is a valid multihash", func(t *testing.T) {
		pid := peerIDPlaceholderFromArbitraryID("provider-X")
		_, err := multihash.Decode([]byte(pid))
		require.NoError(t, err, "placeholder PeerID should be a valid multihash")
	})
}

// TestPeerIDFromDIDKey tests did:key parsing and PeerID derivation in
// isolation from the FindProviders flow. This covers edge cases (wrong prefix,
// unsupported key types) that are hard to exercise through the integration test.
func TestPeerIDFromDIDKey(t *testing.T) {
	t.Run("valid ed25519 did:key", func(t *testing.T) {
		_, pubKey, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)
		expectedPID, err := peer.IDFromPublicKey(pubKey)
		require.NoError(t, err)

		didKey := encodeDIDKey(t, pubKey)
		pid, err := peerIDFromDIDKey(didKey)
		require.NoError(t, err)
		assert.Equal(t, expectedPID, pid)
	})

	t.Run("not a did:key", func(t *testing.T) {
		_, err := peerIDFromDIDKey("12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a did:key")
	})

	t.Run("unsupported key type", func(t *testing.T) {
		// Encode a fake key with a different multicodec (e.g. secp256k1-pub 0xe7)
		prefix := varint.ToUvarint(uint64(multicodec.Secp256k1Pub))
		fakeKey := make([]byte, 33) // secp256k1 pubkey is 33 bytes
		data := append(prefix, fakeKey...)
		encoded, err := multibase.Encode(multibase.Base58BTC, data)
		require.NoError(t, err)

		_, err = peerIDFromDIDKey("did:key:" + encoded)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported key type")
	})
}
