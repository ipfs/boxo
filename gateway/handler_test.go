package gateway

import (
	"testing"

	"github.com/ipfs/boxo/path"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIpfsUriHeaderValue(t *testing.T) {
	const (
		// CIDv1 already in the canonical lowercase base32 form.
		cidV1 = "bafkreiba3vpkcqpc6xtp3hsatzcod6iwneouzjoq7ymy4m2js6gc3czt6i"
		// The same DAG root in CIDv0 and canonical CIDv1 base32 form.
		cidV0         = "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
		cidV0AsBase32 = "bafybeie5nqv6kd3qnfjupgvz34woh3oksc3iau6abmyajn7qvtf6d2ho34"
		// The same ed25519 IPNS name as a base58 peer ID and as the
		// canonical base36 CIDv1 with libp2p-key codec.
		peerIDBase58 = "12D3KooWA4Xop1JaT3MHxwYMkCepYsv4iPVopMXwCz5iHYdBfeSB"
		ipnsNameB36  = "k51qzi5uqu5dg9ufswxt229ntzdy7p4125xzv5rtyjso89ajdujg6csfxcj260"
	)

	for _, test := range []struct {
		name        string
		contentPath string
		expected    string
		ok          bool
	}{
		// Path segment percent-encoding (IPIP-0548 shared test vectors).
		{"unreserved characters kept as-is", "/ipfs/" + cidV1 + "/plain.txt", "ipfs://" + cidV1 + "/plain.txt", true},
		{"space", "/ipfs/" + cidV1 + "/with space.txt", "ipfs://" + cidV1 + "/with%20space.txt", true},
		{"percent sign", "/ipfs/" + cidV1 + "/100% sure.txt", "ipfs://" + cidV1 + "/100%25%20sure.txt", true},
		{"hash and question mark", "/ipfs/" + cidV1 + "/a#b?c.txt", "ipfs://" + cidV1 + "/a%23b%3Fc.txt", true},
		{"name that already looks percent-encoded", "/ipfs/" + cidV1 + "/Portugal%2C+España=Peninsula Ibérica.txt", "ipfs://" + cidV1 + "/Portugal%252C%2BEspa%C3%B1a%3DPeninsula%20Ib%C3%A9rica.txt", true},
		{"multibyte utf-8", "/ipfs/" + cidV1 + "/łódź.txt", "ipfs://" + cidV1 + "/%C5%82%C3%B3d%C5%BA.txt", true},
		{"emoji", "/ipfs/" + cidV1 + "/emoji🚀.txt", "ipfs://" + cidV1 + "/emoji%F0%9F%9A%80.txt", true},
		// Filenames in major scripts (IPIP-0548 shared test vectors).
		{"chinese", "/ipfs/" + cidV1 + "/你好.txt", "ipfs://" + cidV1 + "/%E4%BD%A0%E5%A5%BD.txt", true},
		{"japanese", "/ipfs/" + cidV1 + "/ファイル.txt", "ipfs://" + cidV1 + "/%E3%83%95%E3%82%A1%E3%82%A4%E3%83%AB.txt", true},
		{"korean", "/ipfs/" + cidV1 + "/파일.txt", "ipfs://" + cidV1 + "/%ED%8C%8C%EC%9D%BC.txt", true},
		{"arabic", "/ipfs/" + cidV1 + "/ملف.txt", "ipfs://" + cidV1 + "/%D9%85%D9%84%D9%81.txt", true},
		{"hebrew", "/ipfs/" + cidV1 + "/קובץ.txt", "ipfs://" + cidV1 + "/%D7%A7%D7%95%D7%91%D7%A5.txt", true},
		{"cyrillic", "/ipfs/" + cidV1 + "/файл.txt", "ipfs://" + cidV1 + "/%D1%84%D0%B0%D0%B9%D0%BB.txt", true},
		{"greek", "/ipfs/" + cidV1 + "/αρχείο.txt", "ipfs://" + cidV1 + "/%CE%B1%CF%81%CF%87%CE%B5%CE%AF%CE%BF.txt", true},
		{"devanagari", "/ipfs/" + cidV1 + "/नमस्ते.txt", "ipfs://" + cidV1 + "/%E0%A4%A8%E0%A4%AE%E0%A4%B8%E0%A5%8D%E0%A4%A4%E0%A5%87.txt", true},
		{"thai", "/ipfs/" + cidV1 + "/ไฟล์.txt", "ipfs://" + cidV1 + "/%E0%B9%84%E0%B8%9F%E0%B8%A5%E0%B9%8C.txt", true},
		{"multiple segments", "/ipfs/" + cidV1 + "/subdir/with space.txt", "ipfs://" + cidV1 + "/subdir/with%20space.txt", true},
		// Path mirroring: the URI path reproduces the content path remainder,
		// including a trailing slash. Dot segments and duplicate slashes are
		// collapsed by path.NewPath, mirroring gateway request normalization.
		{"trailing slash on directory kept", "/ipfs/" + cidV1 + "/subdir/", "ipfs://" + cidV1 + "/subdir/", true},
		{"root only with trailing slash", "/ipfs/" + cidV1 + "/", "ipfs://" + cidV1 + "/", true},
		{"single dot segment collapsed", "/ipfs/" + cidV1 + "/a/./b", "ipfs://" + cidV1 + "/a/b", true},
		{"double dot segment collapsed", "/ipfs/" + cidV1 + "/a/../b", "ipfs://" + cidV1 + "/b", true},
		{"duplicate slashes collapsed", "/ipfs/" + cidV1 + "//a//b", "ipfs://" + cidV1 + "/a/b", true},
		// Authority normalization.
		{"cidv0 root normalized to base32 cidv1", "/ipfs/" + cidV0 + "/łódź.txt", "ipfs://" + cidV0AsBase32 + "/%C5%82%C3%B3d%C5%BA.txt", true},
		{"root only", "/ipfs/" + cidV1, "ipfs://" + cidV1, true},
		{"ipns peer id normalized to base36 cidv1", "/ipns/" + peerIDBase58, "ipns://" + ipnsNameB36, true},
		{"ipns name already canonical", "/ipns/" + ipnsNameB36 + "/sub", "ipns://" + ipnsNameB36 + "/sub", true},
		{"dnslink", "/ipns/en.wikipedia-on-ipfs.org/wiki", "ipns://en.wikipedia-on-ipfs.org/wiki", true},
		{"dnslink lowercased", "/ipns/EN.WIKIPEDIA-ON-IPFS.ORG/wiki", "ipns://en.wikipedia-on-ipfs.org/wiki", true},
		{"dnslink trailing dot stripped", "/ipns/en.wikipedia-on-ipfs.org./wiki", "ipns://en.wikipedia-on-ipfs.org/wiki", true},
		{"dnslink unicode converted to a-labels", "/ipns/ŻÓŁĆ.example.net/wiki", "ipns://xn--kda4b0koi.example.net/wiki", true},
		{"dnslink on a private network", "/ipns/example.local/wiki", "ipns://example.local/wiki", true},
		// Roots that cannot be normalized: header is omitted.
		{"ipld namespace has no uri scheme", "/ipld/" + cidV1, "", false},
		{"ipns root neither name nor dnslink", "/ipns/notavalidname", "", false},
		{"dnslink with no dot", "/ipns/examplemissingtld", "", false},
		{"dnslink with empty label", "/ipns/en..example.net/wiki", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			contentPath, err := path.NewPath(test.contentPath)
			require.NoError(t, err)
			value, ok := ipfsUriHeaderValue(contentPath)
			assert.Equal(t, test.ok, ok)
			assert.Equal(t, test.expected, value)
		})
	}
}

func TestEncodeIpfsUriSegment(t *testing.T) {
	// Dot segments never survive path.NewPath, so the encoder is tested
	// directly: IPIP-0548 requires them fully percent-encoded.
	assert.Equal(t, "%2E", encodeIpfsUriSegment("."))
	assert.Equal(t, "%2E%2E", encodeIpfsUriSegment(".."))
	// Dots elsewhere are unreserved and stay as-is.
	assert.Equal(t, "...a", encodeIpfsUriSegment("...a"))
	assert.Equal(t, "a.b", encodeIpfsUriSegment("a.b"))
}

func TestIsFieldValueSafe(t *testing.T) {
	for _, test := range []struct {
		name string
		in   string
		safe bool
	}{
		{"empty", "", true},
		{"visible ascii", "/ipfs/bafkreiba3vpkcqpc6xtp3hsatzcod6iwneouzjoq7ymy4m2js6gc3czt6i/file.txt", true},
		{"htab", "a\tb", true},
		{"space", "a b", true},
		{"boundary 0x21 and 0x7e", "!~", true},
		{"cr", "a\rb", false},
		{"lf", "a\nb", false},
		{"nul", "a\x00b", false},
		{"del 0x7f", "a\x7fb", false},
		{"other control 0x1b", "a\x1bb", false},
		{"utf-8 multibyte", "łódź.txt", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.safe, isFieldValueSafe(test.in))
		})
	}
}

func TestEtagMatch(t *testing.T) {
	for _, test := range []struct {
		header   string // value in If-None-Match HTTP header
		cidEtag  string
		dirEtag  string
		expected bool // expected result of etagMatch(header, cidEtag, dirEtag)
	}{
		{"", `"etag"`, "", false},                        // no If-None-Match
		{"", "", `"etag"`, false},                        // no If-None-Match
		{`"etag"`, `"etag"`, "", true},                   // file etag match
		{`W/"etag"`, `"etag"`, "", true},                 // file etag match (weak)
		{`"foo", W/"bar", W/"etag"`, `"etag"`, "", true}, // file etag match (array)
		{`"foo",W/"bar",W/"etag"`, `"etag"`, "", true},   // file etag match (compact array)
		{`"etag"`, "", `W/"etag"`, true},                 // dir etag match
		{`"etag"`, "", `W/"etag"`, true},                 // dir etag match
		{`W/"etag"`, "", `W/"etag"`, true},               // dir etag match
		{`*`, `"etag"`, "", true},                        // wildcard etag match

		// Bare CID as weak ETag should match against cidEtag even when
		// the response carries a DirIndex ETag. This works because
		// handleIfNoneMatch checks If-None-Match against both cidEtag
		// and dirEtag, and W/"<CID>" weak-matches the bare CID ETag.
		// This is a boxo-specific optimization: the CID match is
		// inexpensive and happens before any I/O.
		{`W/"CID"`, `"CID"`, `"DirIndex-xxhash_CID-CID"`, true}, // bare CID etag matches cidEtag for dir listing
		{`"CID"`, `"CID"`, `"DirIndex-xxhash_CID-CID"`, true},   // strong bare CID also matches
	} {
		result := etagMatch(test.header, test.cidEtag, test.dirEtag)
		assert.Equalf(t, test.expected, result, "etagMatch(%q, %q, %q)", test.header, test.cidEtag, test.dirEtag)
	}
}
