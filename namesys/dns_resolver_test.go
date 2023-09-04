package namesys

import (
	"context"
	"fmt"
	"testing"

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
		return nil, fmt.Errorf("no TXT entry for %s", name)
	}
	return txt, nil
}

func newMockDNS() *mockDNS {
	return &mockDNS{
		entries: map[string][]string{
			"multihash.example.com.": {
				"dnslink=QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			},
			"ipfs.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			},
			"_dnslink.dipfs.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			},
			"dns1.example.com.": {
				"dnslink=/ipns/ipfs.example.com",
			},
			"dns2.example.com.": {
				"dnslink=/ipns/dns1.example.com",
			},
			"multi.example.com.": {
				"some stuff",
				"dnslink=/ipns/dns1.example.com",
				"masked dnslink=/ipns/example.invalid",
			},
			"equals.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/=equals",
			},
			"loop1.example.com.": {
				"dnslink=/ipns/loop2.example.com",
			},
			"loop2.example.com.": {
				"dnslink=/ipns/loop1.example.com",
			},
			"_dnslink.dloop1.example.com.": {
				"dnslink=/ipns/loop2.example.com",
			},
			"_dnslink.dloop2.example.com.": {
				"dnslink=/ipns/loop1.example.com",
			},
			"bad.example.com.": {
				"dnslink=",
			},
			"withsegment.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/sub/segment",
			},
			"withrecsegment.example.com.": {
				"dnslink=/ipns/withsegment.example.com/subsub",
			},
			"withtrailing.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD/sub/",
			},
			"withtrailingrec.example.com.": {
				"dnslink=/ipns/withtrailing.example.com/segment/",
			},
			"double.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			},
			"_dnslink.double.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			},
			"double.conflict.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD",
			},
			"_dnslink.conflict.example.com.": {
				"dnslink=/ipfs/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjE",
			},
			"fqdn.example.com.": {
				"dnslink=/ipfs/QmYvMB9yrsSf7RKBghkfwmHJkzJhW2ZgVwq3LxBXXPasFr",
			},
			"en.wikipedia-on-ipfs.org.": {
				"dnslink=/ipfs/bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze",
			},
			"custom.non-icann.tldextravaganza.": {
				"dnslink=/ipfs/bafybeieto6mcuvqlechv4iadoqvnffondeiwxc2bcfcewhvpsd2odvbmvm",
			},
			"singlednslabelshouldbeok.": {
				"dnslink=/ipfs/bafybeih4a6ylafdki6ailjrdvmr7o4fbbeceeeuty4v3qyyouiz5koqlpi",
			},
			"www.wealdtech.eth.": {
				"dnslink=/ipns/ipfs.example.com",
			},
		},
	}
}

func TestDNSResolution(t *testing.T) {
	t.Parallel()
	r := &DNSResolver{lookupTXT: newMockDNS().lookupTXT}

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
