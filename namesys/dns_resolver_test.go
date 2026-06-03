package namesys

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDNSParseEntry(t *testing.T) {
	t.Parallel()

	t.Run("Valid entries", func(t *testing.T) {
		t.Parallel()

		for _, entry := range []string{
			"QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			"dnslink=/ipns/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/foo",
			"dnslink=/ipns/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/bar",
			"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/foo/bar/baz",
			"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/foo/bar/baz/",
			"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
		} {
			_, err := parseEntry(entry)
			assert.NoError(t, err)
		}
	})

	t.Run("Invalid entries", func(t *testing.T) {
		t.Parallel()

		for _, entry := range []string{
			"QmYhE8xgFCjGcz6PHgnvJz5NOTCORRECT",
			"quux=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			"dnslink=",
			"dnslink=/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/foo",
			"dnslink=ipns/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/bar",
		} {
			_, err := parseEntry(entry)
			assert.Error(t, err)
		}
	})
}

type mockDNS struct {
	entries map[string][]string
}

func (m *mockDNS) lookupTXT(ctx context.Context, name string) (txt []string, err error) {
	txt, ok := m.entries[name]
	if !ok {
		return nil, &net.DNSError{IsNotFound: true}
	}
	return txt, nil
}

func newMockDNS() *mockDNS {
	return &mockDNS{
		entries: map[string][]string{
			"_dnslink.multihash.example.com.": {
				"dnslink=QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			},
			"_dnslink.ipfs.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			},
			"_dnslink.dipfs.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			},
			"_dnslink.dns1.example.com.": {
				"dnslink=/ipns/ipfs.example.com",
			},
			"_dnslink.dns2.example.com.": {
				"dnslink=/ipns/dns1.example.com",
			},
			"_dnslink.multi.example.com.": {
				"some stuff",
				"dnslink=/ipns/dns1.example.com",
				"masked dnslink=/ipns/example.invalid",
			},
			"_dnslink.multi-invalid.example.com.": {
				"some stuff",
				"dnslink=/ipns/dns1.example.com", // we must error when >1 value with /ipns or /ipfs exists
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
				"broken dnslink=/ipns/example.invalid",
			},
			"_dnslink.multi-valid.example.com.": {
				"some stuff",
				"dnslink=/foo/bar", // duplicate dnslink= is fine as long it is not /ipfs or /ipns, which must be unique
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
				"broken dnslink=/ipns/example.invalid",
			},
			"_dnslink.equals.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/=equals",
			},
			"_dnslink.loop1.example.com.": {
				"dnslink=/ipns/loop2.example.com",
			},
			"_dnslink.loop2.example.com.": {
				"dnslink=/ipns/loop1.example.com",
			},
			"_dnslink.dloop1.example.com.": {
				"dnslink=/ipns/loop2.example.com",
			},
			"_dnslink.dloop2.example.com.": {
				"dnslink=/ipns/loop1.example.com",
			},
			"_dnslink.bad.example.com.": {
				"dnslink=",
			},
			"_dnslink.withsegment.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/sub/segment",
			},
			"_dnslink.withrecsegment.example.com.": {
				"dnslink=/ipns/withsegment.example.com/subsub",
			},
			"_dnslink.withtrailing.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/sub/",
			},
			"_dnslink.withtrailingrec.example.com.": {
				"dnslink=/ipns/withtrailing.example.com/segment/",
			},
			"_dnslink.double.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			},
			"_dnslink.double.conflict.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			},
			"_dnslink.conflict.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjE",
			},
			"_dnslink.fqdn.example.com.": {
				"dnslink=/ipfs/QmYvMB9yrsSf7RKBghkfwmHJkzJhW2ZgVwq3LxBXXPasFr",
			},
			"_dnslink.en.wikipedia-on-ipfs.org.": {
				"dnslink=/ipfs/bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze",
			},
			"_dnslink.custom.non-icann.tldextravaganza.": {
				"dnslink=/ipfs/bafybeieto6mcuvqlechv4iadoqvnffondeiwxc2bcfcewhvpsd2odvbmvm",
			},
			"_dnslink.singlednslabelshouldbeok.": {
				"dnslink=/ipfs/bafybeih4a6ylafdki6ailjrdvmr7o4fbbeceeeuty4v3qyyouiz5koqlpi",
			},
			"_dnslink.www.wealdtech.eth.": {
				"dnslink=/ipns/ipfs.example.com",
			},
		},
	}
}

func TestDNSResolution(t *testing.T) {
	t.Parallel()
	r := NewDNSResolver(newMockDNS().lookupTXT)

	for _, testCase := range []struct {
		name          string
		depth         uint
		expectedPath  string
		expectedError error
	}{
		{"/ipns/multihash.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", nil},
		{"/ipns/ipfs.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", nil},
		{"/ipns/dipfs.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", nil},
		{"/ipns/dns1.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", nil},
		{"/ipns/dns1.example.com", 1, "/ipns/ipfs.example.com", ErrResolveRecursion},
		{"/ipns/dns2.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", nil},
		{"/ipns/dns2.example.com", 1, "/ipns/dns1.example.com", ErrResolveRecursion},
		{"/ipns/dns2.example.com", 2, "/ipns/ipfs.example.com", ErrResolveRecursion},
		{"/ipns/multi.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", nil},
		{"/ipns/multi.example.com", 1, "/ipns/dns1.example.com", ErrResolveRecursion},
		{"/ipns/multi.example.com", 2, "/ipns/ipfs.example.com", ErrResolveRecursion},
		{"/ipns/multi-invalid.example.com", 2, "", ErrMultipleDNSLinkRecords},
		{"/ipns/multi-valid.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", nil},
		{"/ipns/equals.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/=equals", nil},
		{"/ipns/loop1.example.com", 1, "/ipns/loop2.example.com", ErrResolveRecursion},
		{"/ipns/loop1.example.com", 2, "/ipns/loop1.example.com", ErrResolveRecursion},
		{"/ipns/loop1.example.com", 3, "/ipns/loop2.example.com", ErrResolveRecursion},
		{"/ipns/loop1.example.com", DefaultDepthLimit, "/ipns/loop1.example.com", ErrResolveRecursion},
		{"/ipns/dloop1.example.com", 1, "/ipns/loop2.example.com", ErrResolveRecursion},
		{"/ipns/dloop1.example.com", 2, "/ipns/loop1.example.com", ErrResolveRecursion},
		{"/ipns/dloop1.example.com", 3, "/ipns/loop2.example.com", ErrResolveRecursion},
		{"/ipns/dloop1.example.com", DefaultDepthLimit, "/ipns/loop1.example.com", ErrResolveRecursion},
		{"/ipns/bad.example.com", DefaultDepthLimit, "", ErrResolveFailed},
		{"/ipns/bad.example.com", DefaultDepthLimit, "", ErrResolveFailed},
		{"/ipns/withsegment.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/sub/segment", nil},
		{"/ipns/withrecsegment.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/sub/segment/subsub", nil},
		{"/ipns/withsegment.example.com/test1", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/sub/segment/test1", nil},
		{"/ipns/withrecsegment.example.com/test2", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/sub/segment/subsub/test2", nil},
		{"/ipns/withrecsegment.example.com/test3/", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/sub/segment/subsub/test3/", nil},
		{"/ipns/withtrailingrec.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/sub/segment/", nil},
		{"/ipns/double.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", nil},
		{"/ipns/conflict.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjE", nil},
		{"/ipns/fqdn.example.com.", DefaultDepthLimit, "/ipfs/QmYvMB9yrsSf7RKBghkfwmHJkzJhW2ZgVwq3LxBXXPasFr", nil},
		{"/ipns/en.wikipedia-on-ipfs.org", 2, "/ipfs/bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze", nil},
		{"/ipns/custom.non-icann.tldextravaganza.", 2, "/ipfs/bafybeieto6mcuvqlechv4iadoqvnffondeiwxc2bcfcewhvpsd2odvbmvm", nil},
		{"/ipns/singlednslabelshouldbeok", 2, "/ipfs/bafybeih4a6ylafdki6ailjrdvmr7o4fbbeceeeuty4v3qyyouiz5koqlpi", nil},
		{"/ipns/www.wealdtech.eth", 1, "/ipns/ipfs.example.com", ErrResolveRecursion},
		{"/ipns/www.wealdtech.eth", 2, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", nil},
		{"/ipns/www.wealdtech.eth", 2, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", nil},
		{"/ipns/www.wealdtech.eth", 2, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", nil},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			testResolution(t, r, testCase.name, (testCase.depth), testCase.expectedPath, 0, testCase.expectedError)
		})
	}
}

func TestDNSResolutionWithTTL(t *testing.T) {
	t.Parallel()

	const ttl = 42 * time.Second
	lookup := func(ctx context.Context, name string) ([]string, time.Duration, error) {
		if name == "_dnslink.ttl.example.com." {
			return []string{"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD"}, ttl, nil
		}
		return nil, 0, &net.DNSError{IsNotFound: true}
	}

	// the TTL-aware resolver propagates the record TTL to the resolved result
	r := NewDNSResolverWithTTL(lookup)
	testResolution(t, r, "/ipns/ttl.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", ttl, nil)

	// the legacy constructor reports an unknown TTL (0) for the same lookup
	rNoTTL := NewDNSResolver(func(ctx context.Context, name string) ([]string, error) {
		txt, _, err := lookup(ctx, name)
		return txt, err
	})
	testResolution(t, rNoTTL, "/ipns/ttl.example.com", DefaultDepthLimit, "/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", 0, nil)
}

func TestMinNonZeroTTL(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		a, b, want time.Duration
	}{
		{0, 0, 0},                                            // both unknown -> unknown
		{0, 5 * time.Second, 5 * time.Second},                // one unknown -> the other
		{5 * time.Second, 0, 5 * time.Second},                // one unknown -> the other
		{3 * time.Second, 5 * time.Second, 3 * time.Second},  // both known -> min
		{5 * time.Second, 3 * time.Second, 3 * time.Second},  // both known -> min
		{-1 * time.Second, 5 * time.Second, 5 * time.Second}, // negative ignored -> the other
		{-3 * time.Second, -5 * time.Second, 0},              // both negative -> never negative
		{-1 * time.Second, 0, 0},                             // negative and unknown -> never negative
	} {
		if got := minNonZeroTTL(tc.a, tc.b); got != tc.want {
			t.Fatalf("minNonZeroTTL(%s, %s) = %s; want %s", tc.a, tc.b, got, tc.want)
		}
	}
}
