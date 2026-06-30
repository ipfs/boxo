package path

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPathFromURI(t *testing.T) {
	t.Parallel()

	t.Run("Valid URIs", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			src       string
			canonical string
			namespace string
			mutable   bool
		}{
			// ipfs:// with CIDv1, with and without a sub-path
			{"ipfs://bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", "/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", IPFSNamespace, false},
			{"ipfs://bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b", "/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b", IPFSNamespace, false},

			// CIDv0 is base58 and case-sensitive: the root must survive untouched.
			{"ipfs://QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", IPFSNamespace, false},
			{"ipfs://QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", IPFSNamespace, false},

			// ipns:// with an IPNS key and with a DNSLink name
			{"ipns://bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", "/ipns/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", IPNSNamespace, true},
			{"ipns://docs.ipfs.tech/concepts", "/ipns/docs.ipfs.tech/concepts", IPNSNamespace, true},

			// ipld:// maps to the /ipld namespace
			{"ipld://bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a", "/ipld/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a", IPLDNamespace, false},

			// Schemeless forms: ipfs:{cid}, ipns:{name}, ipld:{cid}
			{"ipfs:bafkqaaa", "/ipfs/bafkqaaa", IPFSNamespace, false},
			{"ipns:docs.ipfs.tech", "/ipns/docs.ipfs.tech", IPNSNamespace, true},
			{"ipld:bafkqaaa", "/ipld/bafkqaaa", IPLDNamespace, false},

			// Scheme is case-insensitive (RFC 3986); the rest is preserved.
			{"IPFS://bafkqaaa", "/ipfs/bafkqaaa", IPFSNamespace, false},
			{"IpNs://docs.ipfs.tech", "/ipns/docs.ipfs.tech", IPNSNamespace, true},

			// Trailing slash is preserved, mirroring /ipfs/cid/ behaviour.
			{"ipfs://bafkqaaa/", "/ipfs/bafkqaaa/", IPFSNamespace, false},

			// Cleaning still applies after normalization.
			{"ipfs://bafkqaaa/a/b/../c", "/ipfs/bafkqaaa/a/c", IPFSNamespace, false},

			// A canonical path passes straight through to NewPath.
			{"/ipfs/bafkqaaa", "/ipfs/bafkqaaa", IPFSNamespace, false},
			{"/ipns/docs.ipfs.tech", "/ipns/docs.ipfs.tech", IPNSNamespace, true},
		}

		for _, testCase := range testCases {
			p, err := NewPathFromURI(testCase.src)
			assert.NoErrorf(t, err, "input %q", testCase.src)
			if err != nil {
				continue
			}
			assert.Equalf(t, testCase.canonical, p.String(), "input %q", testCase.src)
			assert.Equalf(t, testCase.namespace, p.Namespace(), "input %q", testCase.src)
			assert.Equalf(t, testCase.mutable, p.Mutable(), "input %q", testCase.src)
		}
	})

	t.Run("Invalid URIs still error", func(t *testing.T) {
		t.Parallel()

		// Empty-after-scheme reduces to "/ipfs/" etc., which NewPath rejects.
		for _, src := range []string{"ipfs:", "ipns:", "ipld:", "ipfs://", "ipns://", "IPFS://"} {
			_, err := NewPathFromURI(src)
			assert.ErrorIsf(t, err, ErrInsufficientComponents, "input %q", src)
			assert.ErrorIsf(t, err, &ErrInvalidPath{}, "input %q always an ErrInvalidPath", src)
		}

		// A malformed CID still fails (as an ErrInvalidPath wrapping the CID error).
		_, err := NewPathFromURI("ipfs://notacid")
		assert.ErrorIs(t, err, &ErrInvalidPath{})
	})

	t.Run("Returns ImmutablePath for ipfs:// URIs", func(t *testing.T) {
		t.Parallel()

		p, err := NewPathFromURI("ipfs://bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a")
		assert.NoError(t, err)
		assert.IsType(t, ImmutablePath{}, p)
	})

	// A literal value that merely starts with "ipns"/"ipfs" but has no ":" right
	// after the namespace must not be rewritten.
	t.Run("Does not misfire on non-URI input", func(t *testing.T) {
		t.Parallel()

		for _, src := range []string{"ipfsfile", "ipns-notes.txt", "ipfsbar:baz", "ipns", "/ipfs/bafkqaaa"} {
			assert.Equalf(t, src, normalizeURIScheme(src), "input %q must be unchanged", src)
		}
	})
}

// TestNewPathStaysStrict guards the security boundary: NewPath itself must keep
// rejecting native IPFS URIs, so contexts that parse untrusted strings (such as
// DNSLink records) are not loosened. Only NewPathFromURI accepts URIs.
func TestNewPathStaysStrict(t *testing.T) {
	t.Parallel()

	for _, src := range []string{
		"ipfs://bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku",
		"ipns://attacker.example",
		"ipfs:bafkqaaa",
		"ipns:attacker.example",
	} {
		_, err := NewPath(src)
		assert.Errorf(t, err, "NewPath must reject URI %q", src)
		assert.ErrorIsf(t, err, &ErrInvalidPath{}, "input %q", src)
	}
}
