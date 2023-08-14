package path

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPath(t *testing.T) {
	t.Run("Valid Paths", func(t *testing.T) {
		testCases := []struct {
			src       string
			canonical string
			namespace Namespace
			mutable   bool
		}{
			// IPFS CIDv0
			{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", IPFSNamespace, false},
			{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", IPFSNamespace, false},
			{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/c/d/e/f", "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/c/d/e/f", IPFSNamespace, false},

			// IPFS CIDv1
			{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", "/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", IPFSNamespace, false},
			{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a", "/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a", IPFSNamespace, false},
			{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b/c/d/e/f", "/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b/c/d/e/f", IPFSNamespace, false},

			// IPLD CIDv0
			{"/ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", "/ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", IPLDNamespace, false},
			{"/ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", "/ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", IPLDNamespace, false},
			{"/ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/c/d/e/f", "/ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/c/d/e/f", IPLDNamespace, false},

			// IPLD CIDv1
			{"/ipld/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", "/ipld/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", IPLDNamespace, false},
			{"/ipld/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a", "/ipld/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a", IPLDNamespace, false},
			{"/ipld/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b/c/d/e/f", "/ipld/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b/c/d/e/f", IPLDNamespace, false},

			// IPNS CIDv0
			{"/ipns/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", "/ipns/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", IPNSNamespace, true},
			{"/ipns/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", "/ipns/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", IPNSNamespace, true},
			{"/ipns/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/c/d/e/f", "/ipns/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/c/d/e/f", IPNSNamespace, true},

			// IPNS CIDv1
			{"/ipns/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", "/ipns/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", IPNSNamespace, true},
			{"/ipns/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a", "/ipns/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a", IPNSNamespace, true},
			{"/ipns/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b/c/d/e/f", "/ipns/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b/c/d/e/f", IPNSNamespace, true},

			// IPNS DNSLink
			{"/ipns/domain.net", "/ipns/domain.net", IPNSNamespace, true},
			{"/ipns/domain.net/a/b/c/d", "/ipns/domain.net/a/b/c/d", IPNSNamespace, true},

			// Cleaning checks
			{"/ipfs/bafkqaaa/", "/ipfs/bafkqaaa/", IPFSNamespace, false},
			{"/ipfs/bafkqaaa//", "/ipfs/bafkqaaa/", IPFSNamespace, false},
			{"/ipfs///bafkqaaa//", "/ipfs/bafkqaaa/", IPFSNamespace, false},
			{"/ipfs///bafkqaaa/a/b/../c", "/ipfs/bafkqaaa/a/c", IPFSNamespace, false},
			{"/ipfs///bafkqaaa/a/b/../c/", "/ipfs/bafkqaaa/a/c/", IPFSNamespace, false},
		}

		for _, testCase := range testCases {
			p, err := NewPath(testCase.src)
			assert.NoError(t, err)
			assert.Equal(t, testCase.canonical, p.String())
			assert.Equal(t, testCase.namespace, p.Namespace())
			assert.Equal(t, testCase.mutable, p.Namespace().Mutable())
		}
	})

	t.Run("Invalid Paths", func(t *testing.T) {
		testCases := []struct {
			src string
			err error
		}{
			{"QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", ErrInvalidPath{}},
			{"QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", ErrInvalidPath{}},
			{"bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a", ErrInvalidPath{}},
			{"/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", ErrInvalidPath{}},
			{"/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", ErrInvalidPath{}},
			{"/ipfs/foo", ErrInvalidPath{}},
			{"/ipfs/", ErrInvalidPath{}},
			{"ipfs/", ErrInvalidPath{}},
			{"ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", ErrInvalidPath{}},
			{"/ipld/foo", ErrInvalidPath{}},
			{"/ipld/", ErrInvalidPath{}},
			{"ipld/", ErrInvalidPath{}},
			{"ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", ErrInvalidPath{}},
			{"/ipns", ErrInvalidPath{}},
			{"/ipfs/", ErrInvalidPath{}},
			{"/ipns/", ErrInvalidPath{}},
			{"/ipld/", ErrInvalidPath{}},
			{"/ipfs", ErrInvalidPath{}},
			{"/testfs", ErrInvalidPath{}},
			{"/", ErrInvalidPath{}},
		}

		for _, testCase := range testCases {
			_, err := NewPath(testCase.src)
			assert.ErrorIs(t, err, testCase.err)
		}
	})

	t.Run("Returns ImmutablePath for IPFS and IPLD Paths", func(t *testing.T) {
		testCases := []struct {
			src string
		}{
			{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"},
			{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a"},
			{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b/c/d/e/f"},
			{"/ipld/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"},
			{"/ipld/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a"},
			{"/ipld/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b/c/d/e/f"},
		}

		for _, testCase := range testCases {
			p, err := NewPath(testCase.src)
			assert.NoError(t, err)
			assert.IsType(t, immutablePath{}, p)
		}
	})
}
