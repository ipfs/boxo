package gateway

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
