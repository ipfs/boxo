package path

import (
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
)

func newIPLDPath(cid cid.Cid) ImmutablePath {
	return ImmutablePath{
		path: path{
			str:       fmt.Sprintf("/%s/%s", IPLDNamespace, cid.String()),
			namespace: IPLDNamespace,
		},
		rootCid: cid,
	}
}

func TestNewPath(t *testing.T) {
	t.Parallel()

	t.Run("Valid Paths", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			src       string
			canonical string
			namespace string
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
			assert.Equal(t, testCase.mutable, p.Mutable())
		}
	})

	t.Run("Invalid Paths", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			src string
			err error
		}{
			{"QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", ErrInsufficientComponents},
			{"QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", ErrInsufficientComponents},
			{"bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a", ErrInsufficientComponents},
			{"/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", ErrInsufficientComponents},
			{"/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", ErrUnknownNamespace},
			{"/ipfs/foo", cid.ErrInvalidCid{}},
			{"/ipfs/", ErrInsufficientComponents},
			{"ipfs/", ErrInsufficientComponents},
			{"ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", ErrInsufficientComponents},
			{"/ipld/foo", &ErrInvalidPath{}},
			{"/ipld/", ErrInsufficientComponents},
			{"ipld/", ErrInsufficientComponents},
			{"ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", ErrInsufficientComponents},
			{"/ipns", ErrInsufficientComponents},
			{"/ipfs/", ErrInsufficientComponents},
			{"/ipns/", ErrInsufficientComponents},
			{"/ipld/", ErrInsufficientComponents},
			{"/ipfs", ErrInsufficientComponents},
			{"/testfs", ErrInsufficientComponents},
			{"/", ErrInsufficientComponents},
		}

		for _, testCase := range testCases {
			_, err := NewPath(testCase.src)
			assert.ErrorIs(t, err, testCase.err)
			assert.ErrorIs(t, err, &ErrInvalidPath{}) // Always an ErrInvalidPath!
		}
	})

	t.Run("Returns ImmutablePath for IPFS and IPLD Paths", func(t *testing.T) {
		t.Parallel()

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
			assert.IsType(t, ImmutablePath{}, p)
		}
	})
}

func TestFromCid(t *testing.T) {
	t.Parallel()

	t.Run("Works with CIDv0", func(t *testing.T) {
		t.Parallel()

		c, err := cid.Decode("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n")
		assert.NoError(t, err)

		p := FromCid(c)
		assert.IsType(t, ImmutablePath{}, p)
		assert.Equal(t, "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", p.String())
		assert.Equal(t, c, p.RootCid())
	})

	t.Run("Works with CIDv1", func(t *testing.T) {
		t.Parallel()

		c, err := cid.Decode("bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")
		assert.NoError(t, err)

		p := FromCid(c)
		assert.IsType(t, ImmutablePath{}, p)
		assert.Equal(t, "/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", p.String())
		assert.Equal(t, c, p.RootCid())
	})

	t.Run("newIPLDPath returns correct ImmutablePath", func(t *testing.T) {
		c, err := cid.Decode("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n")
		assert.NoError(t, err)

		p := newIPLDPath(c)
		assert.IsType(t, ImmutablePath{}, p)
		assert.Equal(t, "/ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", p.String())
		assert.Equal(t, c, p.RootCid())

		// Check if CID encoding is preserved.
		c, err = cid.Decode("bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")
		assert.NoError(t, err)

		p = newIPLDPath(c)
		assert.IsType(t, ImmutablePath{}, p)
		assert.Equal(t, "/ipld/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", p.String())
		assert.Equal(t, c, p.RootCid())
	})
}

func TestNewImmutablePath(t *testing.T) {
	t.Parallel()

	t.Run("Fails on Mutable Path", func(t *testing.T) {
		for _, path := range []string{
			"/ipns/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n",
			"/ipns/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku",
			"/ipns/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/with/path",
			"/ipns/domain.net",
		} {
			p, err := NewPath(path)
			assert.NoError(t, err)

			_, err = NewImmutablePath(p)
			assert.ErrorIs(t, err, ErrExpectedImmutable)
			assert.ErrorIs(t, err, &ErrInvalidPath{})
		}
	})

	t.Run("Succeeds on Immutable Path", func(t *testing.T) {
		testCases := []struct {
			path string
			cid  cid.Cid
		}{
			{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", cid.MustParse("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n")},
			{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b", cid.MustParse("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n")},
			{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/", cid.MustParse("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n")},

			{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", cid.MustParse("bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")},
			{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b", cid.MustParse("bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")},
			{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b/", cid.MustParse("bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")},

			{"/ipld/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", cid.MustParse("bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")},
			{"/ipld/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b", cid.MustParse("bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")},
			{"/ipld/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b/", cid.MustParse("bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")},
		}

		for _, testCase := range testCases {
			p, err := NewPath(testCase.path)
			assert.NoError(t, err)

			ip, err := NewImmutablePath(p)
			assert.NoError(t, err)
			assert.Equal(t, testCase.path, ip.String())
			assert.Equal(t, testCase.cid, ip.RootCid())
		}
	})
}

func TestJoin(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		path     string
		segments []string
		expected string
	}{
		{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", []string{"a/b"}, "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b"},
		{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", []string{"/a/b"}, "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b"},
		{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/", []string{"/a/b"}, "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b"},
		{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", []string{"a", "b"}, "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b"},
		{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", []string{"a/b/../"}, "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/"},
		{"/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", []string{"a/b", "/"}, "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/"},

		{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", []string{"a/b"}, "/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b"},
		{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", []string{"/a/b"}, "/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b"},
		{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/", []string{"/a/b"}, "/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b"},
		{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", []string{"a", "b"}, "/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b"},
		{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", []string{"a/b/../"}, "/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/"},
		{"/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", []string{"a/b", "/"}, "/ipfs/bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku/a/b/"},
	}

	for _, testCase := range testCases {
		p, err := NewPath(testCase.path)
		assert.NoError(t, err)
		jp, err := Join(p, testCase.segments...)
		assert.NoError(t, err)
		assert.Equal(t, testCase.expected, jp.String())
	}
}

func TestStringToSegments(t *testing.T) {
	testCases := []struct {
		str      string
		expected []string
	}{
		{"", nil},
		{"/..", nil},
		{"/a/b/c/d/./../../../../../..", nil},
		{"/a/b/c/d/./../../../", []string{"a"}},
		{"/a/b//c/d/./../../", []string{"a", "b"}},
		{"/a/b/////c/../d///f", []string{"a", "b", "d", "f"}},
	}

	for _, testCase := range testCases {
		segments := StringToSegments(testCase.str)
		assert.Equal(t, testCase.expected, segments)
	}
}

func TestSegmentsToString(t *testing.T) {
	testCases := []struct {
		segments []string
		expected string
	}{
		{[]string{"a", "b"}, "/a/b"},
		{[]string{"a", "b", "d", "f"}, "/a/b/d/f"},
		{[]string{""}, ""},
		{[]string{}, ""},
		{nil, ""},
	}

	for _, testCase := range testCases {
		str := SegmentsToString(testCase.segments...)
		assert.Equal(t, testCase.expected, str)
	}
}
