package gateway

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ipfs/boxo/path"
	cid "github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToSubdomainURL(t *testing.T) {
	t.Parallel()

	backend, _ := newMockBackend(t, "fixtures.car")
	testCID, err := cid.Decode("bafkqaglimvwgy3zakrsxg5cun5jxkyten5wwc2lokvjeycq")
	require.NoError(t, err)

	backend.namesys["/ipns/dnslink.long-name.example.com"] = newMockNamesysItem(path.FromCid(testCID), 0)
	backend.namesys["/ipns/dnslink.too-long.f1siqrebi3vir8sab33hu5vcy008djegvay6atmz91ojesyjs8lx350b7y7i1nvyw2haytfukfyu2f2x4tocdrfa0zgij6p4zpl4u5o.example.com"] = newMockNamesysItem(path.FromCid(testCID), 0)
	httpRequest := httptest.NewRequest("GET", "http://127.0.0.1:8080", nil)
	httpsRequest := httptest.NewRequest("GET", "https://https-request-stub.example.com", nil)
	httpsProxiedRequest := httptest.NewRequest("GET", "http://proxied-https-request-stub.example.com", nil)
	httpsProxiedRequest.Header.Set("X-Forwarded-Proto", "https")

	for _, test := range []struct {
		// in:
		request       *http.Request
		gwHostname    string
		inlineDNSLink bool
		path          string
		// out:
		url string
		err error
	}{
		// DNSLink
		{httpRequest, "localhost", false, "/ipns/dnslink.io", "http://dnslink.io.ipns.localhost/", nil},
		// Hostname with port
		{httpRequest, "localhost:8080", false, "/ipns/dnslink.io", "http://dnslink.io.ipns.localhost:8080/", nil},
		// CIDv0 → CIDv1base32
		{httpRequest, "localhost", false, "/ipfs/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", "http://bafybeif7a7gdklt6hodwdrmwmxnhksctcuav6lfxlcyfz4khzl3qfmvcgu.ipfs.localhost/", nil},
		// CIDv1 with long sha512
		{httpRequest, "localhost", false, "/ipfs/bafkrgqe3ohjcjplc6n4f3fwunlj6upltggn7xqujbsvnvyw764srszz4u4rshq6ztos4chl4plgg4ffyyxnayrtdi5oc4xb2332g645433aeg", "", errors.New("CID incompatible with DNS label length limit of 63: kf1siqrebi3vir8sab33hu5vcy008djegvay6atmz91ojesyjs8lx350b7y7i1nvyw2haytfukfyu2f2x4tocdrfa0zgij6p4zpl4u5oj")},
		// PeerID as CIDv1 needs to have libp2p-key multicodec
		{httpRequest, "localhost", false, "/ipns/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", "http://k2k4r8n0flx3ra0y5dr8fmyvwbzy3eiztmtq6th694k5a3rznayp3e4o.ipns.localhost/", nil},
		{httpRequest, "localhost", false, "/ipns/bafybeickencdqw37dpz3ha36ewrh4undfjt2do52chtcky4rxkj447qhdm", "http://k2k4r8l9ja7hkzynavdqup76ou46tnvuaqegbd04a4o1mpbsey0meucb.ipns.localhost/", nil},
		// PeerID: ed25519+identity multihash → CIDv1Base36
		{httpRequest, "localhost", false, "/ipns/12D3KooWFB51PRY9BxcXSH6khFXw1BZeszeLDy7C8GciskqCTZn5", "http://k51qzi5uqu5di608geewp3nqkg0bpujoasmka7ftkyxgcm3fh1aroup0gsdrna.ipns.localhost/", nil},
		{httpRequest, "sub.localhost", false, "/ipfs/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", "http://bafybeif7a7gdklt6hodwdrmwmxnhksctcuav6lfxlcyfz4khzl3qfmvcgu.ipfs.sub.localhost/", nil},
		// HTTPS requires DNSLink name to fit in a single DNS label – see "Option C" from https://github.com/ipfs/in-web-browsers/issues/169
		{httpRequest, "dweb.link", false, "/ipns/dnslink.long-name.example.com", "http://dnslink.long-name.example.com.ipns.dweb.link/", nil},
		{httpsRequest, "dweb.link", false, "/ipns/dnslink.long-name.example.com", "https://dnslink-long--name-example-com.ipns.dweb.link/", nil},
		{httpsProxiedRequest, "dweb.link", false, "/ipns/dnslink.long-name.example.com", "https://dnslink-long--name-example-com.ipns.dweb.link/", nil},
		// Enabling DNS label inlining: HTTP requests can also be converted to fit into a single DNS label when it matters - https://github.com/ipfs/kubo/issues/9243
		{httpRequest, "localhost", true, "/ipns/dnslink.long-name.example.com", "http://dnslink-long--name-example-com.ipns.localhost/", nil},
		{httpRequest, "dweb.link", true, "/ipns/dnslink.long-name.example.com", "http://dnslink-long--name-example-com.ipns.dweb.link/", nil},
		// Disabling DNS label inlining: should un-inline any inlined DNS labels put in a path
		{httpRequest, "localhost", false, "/ipns/dnslink-long--name-example-com", "http://dnslink.long-name.example.com.ipns.localhost/", nil},
		{httpRequest, "dweb.link", false, "/ipns/dnslink-long--name-example-com", "http://dnslink.long-name.example.com.ipns.dweb.link/", nil},
		// Correctly redirects paths when there is a ? (question mark) character - https://github.com/ipfs/kubo/issues/9882
		{httpRequest, "localhost", false, "/ipns/example.com/this is a file with some spaces . dots and - but also a ?.png", "http://example.com.ipns.localhost/this%20is%20a%20file%20with%20some%20spaces%20.%20dots%20and%20-%20but%20also%20a%20%3F.png", nil},
		{httpRequest, "localhost", false, "/ipfs/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n/this is a file with some spaces . dots and - but also a ?.png", "http://bafybeif7a7gdklt6hodwdrmwmxnhksctcuav6lfxlcyfz4khzl3qfmvcgu.ipfs.localhost/this%20is%20a%20file%20with%20some%20spaces%20.%20dots%20and%20-%20but%20also%20a%20%3F.png", nil},
	} {
		testName := fmt.Sprintf("%s, %v, %s", test.gwHostname, test.inlineDNSLink, test.path)
		t.Run(testName, func(t *testing.T) {
			url, err := toSubdomainURL(test.gwHostname, test.path, test.request, test.inlineDNSLink, backend)
			require.Equal(t, test.url, url)
			require.Equal(t, test.err, err)
		})
	}
}

func TestToDNSLinkDNSLabel(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		in  string
		out string
		err error
	}{
		{"dnslink.long-name.example.com", "dnslink-long--name-example-com", nil},
		{"dnslink.too-long.f1siqrebi3vir8sab33hu5vcy008djegvay6atmz91ojesyjs8lx350b7y7i1nvyw2haytfukfyu2f2x4tocdrfa0zgij6p4zpl4u5o.example.com", "", errors.New("DNSLink representation incompatible with DNS label length limit of 63: dnslink-too--long-f1siqrebi3vir8sab33hu5vcy008djegvay6atmz91ojesyjs8lx350b7y7i1nvyw2haytfukfyu2f2x4tocdrfa0zgij6p4zpl4u5o-example-com")},
	} {
		t.Run(test.in, func(t *testing.T) {
			out, err := toDNSLinkDNSLabel(test.in)
			require.Equal(t, test.out, out)
			require.Equal(t, test.err, err)
		})
	}
}

func TestToDNSLinkFQDN(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		in  string
		out string
	}{
		{"singlelabel", "singlelabel"},
		{"docs-ipfs-tech", "docs.ipfs.tech"},
		{"dnslink-long--name-example-com", "dnslink.long-name.example.com"},
	} {
		t.Run(test.in, func(t *testing.T) {
			out := toDNSLinkFQDN(test.in)
			require.Equal(t, test.out, out)
		})
	}
}

func TestIsHTTPSRequest(t *testing.T) {
	t.Parallel()
	httpRequest := httptest.NewRequest("GET", "http://127.0.0.1:8080", nil)
	httpsRequest := httptest.NewRequest("GET", "https://https-request-stub.example.com", nil)
	httpsProxiedRequest := httptest.NewRequest("GET", "http://proxied-https-request-stub.example.com", nil)
	httpsProxiedRequest.Header.Set("X-Forwarded-Proto", "https")
	httpProxiedRequest := httptest.NewRequest("GET", "http://proxied-http-request-stub.example.com", nil)
	httpProxiedRequest.Header.Set("X-Forwarded-Proto", "http")
	oddballRequest := httptest.NewRequest("GET", "foo://127.0.0.1:8080", nil)
	for _, test := range []struct {
		in  *http.Request
		out bool
	}{
		{httpRequest, false},
		{httpsRequest, true},
		{httpsProxiedRequest, true},
		{httpProxiedRequest, false},
		{oddballRequest, false},
	} {
		testName := fmt.Sprintf("%+v", test.in)
		t.Run(testName, func(t *testing.T) {
			out := isHTTPSRequest(test.in)
			require.Equal(t, test.out, out)
		})
	}
}

func TestHasPrefix(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		prefixes []string
		path     string
		out      bool
	}{
		{[]string{"/ipfs"}, "/ipfs/cid", true},
		{[]string{"/ipfs/"}, "/ipfs/cid", true},
		{[]string{"/version/"}, "/version", true},
		{[]string{"/version"}, "/version", true},
	} {
		testName := fmt.Sprintf("%+v, %s", test.prefixes, test.path)
		t.Run(testName, func(t *testing.T) {
			out := hasPrefix(test.path, test.prefixes...)
			require.Equal(t, test.out, out)
		})
	}
}

func TestIsDomainNameAndNotPeerID(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		hostname string
		out      bool
	}{
		{"", false},
		{"example.com", true},
		{"non-icann.something", true},
		{"..", false},
		{"12D3KooWFB51PRY9BxcXSH6khFXw1BZeszeLDy7C8GciskqCTZn5", false},           // valid peerid
		{"k51qzi5uqu5di608geewp3nqkg0bpujoasmka7ftkyxgcm3fh1aroup0gsdrna", false}, // valid peerid
	} {
		t.Run(test.hostname, func(t *testing.T) {
			out := isDomainNameAndNotPeerID(test.hostname)
			require.Equal(t, test.out, out)
		})
	}
}

func TestPortStripping(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		in  string
		out string
	}{
		{"localhost:8080", "localhost"},
		{"bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am.ipfs.localhost:8080", "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am.ipfs.localhost"},
		{"example.com:443", "example.com"},
		{"example.com", "example.com"},
		{"foo-dweb.ipfs.pvt.k12.ma.us:8080", "foo-dweb.ipfs.pvt.k12.ma.us"},
		{"localhost", "localhost"},
		{"[::1]:8080", "::1"},
	} {
		t.Run(test.in, func(t *testing.T) {
			out := stripPort(test.in)
			require.Equal(t, test.out, out)
		})
	}
}

func TestToDNSLabel(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		in  string
		out string
		err error
	}{
		// <= 63
		{"QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", "QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", nil},
		{"bafybeickencdqw37dpz3ha36ewrh4undfjt2do52chtcky4rxkj447qhdm", "bafybeickencdqw37dpz3ha36ewrh4undfjt2do52chtcky4rxkj447qhdm", nil},
		// > 63
		// PeerID: ed25519+identity multihash → CIDv1Base36
		{"bafzaajaiaejca4syrpdu6gdx4wsdnokxkprgzxf4wrstuc34gxw5k5jrag2so5gk", "k51qzi5uqu5dj16qyiq0tajolkojyl9qdkr254920wxv7ghtuwcz593tp69z9m", nil},
		// CIDv1 with long sha512 → error
		{"bafkrgqe3ohjcjplc6n4f3fwunlj6upltggn7xqujbsvnvyw764srszz4u4rshq6ztos4chl4plgg4ffyyxnayrtdi5oc4xb2332g645433aeg", "", errors.New("CID incompatible with DNS label length limit of 63: kf1siqrebi3vir8sab33hu5vcy008djegvay6atmz91ojesyjs8lx350b7y7i1nvyw2haytfukfyu2f2x4tocdrfa0zgij6p4zpl4u5oj")},
	} {
		t.Run(test.in, func(t *testing.T) {
			inCID, _ := cid.Decode(test.in)
			out, err := toDNSLabel(test.in, inCID)
			require.Equal(t, test.out, out)
			require.Equal(t, test.err, err)
		})
	}
}

func TestKnownSubdomainDetails(t *testing.T) {
	t.Parallel()

	gwLocalhost := &PublicGateway{Paths: []string{"/ipfs", "/ipns", "/api"}, UseSubdomains: true}
	gwDweb := &PublicGateway{Paths: []string{"/ipfs", "/ipns", "/api"}, UseSubdomains: true}
	gwLong := &PublicGateway{Paths: []string{"/ipfs", "/ipns", "/api"}, UseSubdomains: true}
	gwWildcard1 := &PublicGateway{Paths: []string{"/ipfs", "/ipns", "/api"}, UseSubdomains: true}
	gwWildcard2 := &PublicGateway{Paths: []string{"/ipfs", "/ipns", "/api"}, UseSubdomains: true}

	gateways := prepareHostnameGateways(map[string]*PublicGateway{
		"localhost":               gwLocalhost,
		"dweb.link":               gwDweb,
		"devgateway.dweb.link":    gwDweb,
		"dweb.ipfs.pvt.k12.ma.us": gwLong, // note the sneaky ".ipfs." ;-)
		"*.wildcard1.tld":         gwWildcard1,
		"*.*.wildcard2.tld":       gwWildcard2,
	})

	for _, test := range []struct {
		// in:
		hostHeader string
		// out:
		gw       *PublicGateway
		hostname string
		ns       string
		rootID   string
		ok       bool
	}{
		// no subdomain
		{"127.0.0.1:8080", nil, "", "", "", false},
		{"[::1]:8080", nil, "", "", "", false},
		{"hey.look.example.com", nil, "", "", "", false},
		{"dweb.link", nil, "", "", "", false},
		// malformed Host header
		{".....dweb.link", nil, "", "", "", false},
		{"link", nil, "", "", "", false},
		{"8080:dweb.link", nil, "", "", "", false},
		{" ", nil, "", "", "", false},
		{"", nil, "", "", "", false},
		// unknown gateway host
		{"bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am.ipfs.unknown.example.com", nil, "", "", "", false},
		// cid in subdomain, known gateway
		{"bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am.ipfs.localhost:8080", gwLocalhost, "localhost:8080", "ipfs", "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am", true},
		{"bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am.ipfs.dweb.link", gwDweb, "dweb.link", "ipfs", "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am", true},
		{"bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am.ipfs.devgateway.dweb.link", gwDweb, "devgateway.dweb.link", "ipfs", "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am", true},
		// capture everything before .ipfs.
		{"foo.bar.boo-buzz.ipfs.dweb.link", gwDweb, "dweb.link", "ipfs", "foo.bar.boo-buzz", true},
		// ipns
		{"bafzbeihe35nmjqar22thmxsnlsgxppd66pseq6tscs4mo25y55juhh6bju.ipns.localhost:8080", gwLocalhost, "localhost:8080", "ipns", "bafzbeihe35nmjqar22thmxsnlsgxppd66pseq6tscs4mo25y55juhh6bju", true},
		{"bafzbeihe35nmjqar22thmxsnlsgxppd66pseq6tscs4mo25y55juhh6bju.ipns.dweb.link", gwDweb, "dweb.link", "ipns", "bafzbeihe35nmjqar22thmxsnlsgxppd66pseq6tscs4mo25y55juhh6bju", true},
		// edge case check: public gateway under long TLD (see: https://publicsuffix.org)
		{"bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am.ipfs.dweb.ipfs.pvt.k12.ma.us", gwLong, "dweb.ipfs.pvt.k12.ma.us", "ipfs", "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am", true},
		{"bafzbeihe35nmjqar22thmxsnlsgxppd66pseq6tscs4mo25y55juhh6bju.ipns.dweb.ipfs.pvt.k12.ma.us", gwLong, "dweb.ipfs.pvt.k12.ma.us", "ipns", "bafzbeihe35nmjqar22thmxsnlsgxppd66pseq6tscs4mo25y55juhh6bju", true},
		// dnslink in subdomain
		{"en.wikipedia-on-ipfs.org.ipns.localhost:8080", gwLocalhost, "localhost:8080", "ipns", "en.wikipedia-on-ipfs.org", true},
		{"en.wikipedia-on-ipfs.org.ipns.localhost", gwLocalhost, "localhost", "ipns", "en.wikipedia-on-ipfs.org", true},
		{"dist.ipfs.tech.ipns.localhost:8080", gwLocalhost, "localhost:8080", "ipns", "dist.ipfs.tech", true},
		{"en.wikipedia-on-ipfs.org.ipns.dweb.link", gwDweb, "dweb.link", "ipns", "en.wikipedia-on-ipfs.org", true},
		// edge case check: public gateway under long TLD (see: https://publicsuffix.org)
		{"foo.dweb.ipfs.pvt.k12.ma.us", nil, "", "", "", false},
		{"bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am.ipfs.dweb.ipfs.pvt.k12.ma.us", gwLong, "dweb.ipfs.pvt.k12.ma.us", "ipfs", "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am", true},
		{"bafzbeihe35nmjqar22thmxsnlsgxppd66pseq6tscs4mo25y55juhh6bju.ipns.dweb.ipfs.pvt.k12.ma.us", gwLong, "dweb.ipfs.pvt.k12.ma.us", "ipns", "bafzbeihe35nmjqar22thmxsnlsgxppd66pseq6tscs4mo25y55juhh6bju", true},
		// other namespaces
		{"api.localhost", nil, "", "", "", false},
		{"peerid.p2p.localhost", gwLocalhost, "localhost", "p2p", "peerid", true},
		// wildcards
		{"wildcard1.tld", nil, "", "", "", false},
		{".wildcard1.tld", nil, "", "", "", false},
		{"bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am.ipfs.wildcard1.tld", nil, "", "", "", false},
		{"bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am.ipfs.sub.wildcard1.tld", gwWildcard1, "sub.wildcard1.tld", "ipfs", "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am", true},
		{"bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am.ipfs.sub1.sub2.wildcard1.tld", nil, "", "", "", false},
		{"bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am.ipfs.sub1.sub2.wildcard2.tld", gwWildcard2, "sub1.sub2.wildcard2.tld", "ipfs", "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am", true},
	} {
		t.Run(test.hostHeader, func(t *testing.T) {
			gw, hostname, ns, rootID, ok := gateways.knownSubdomainDetails(test.hostHeader)
			assert.Equal(t, test.ok, ok)
			assert.Equal(t, test.rootID, rootID)
			assert.Equal(t, test.ns, ns)
			assert.Equal(t, test.hostname, hostname)
			assert.Equal(t, test.gw, gw)
		})
	}
}
