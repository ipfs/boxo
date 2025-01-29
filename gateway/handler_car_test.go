package gateway

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCarParams(t *testing.T) {
	t.Parallel()

	t.Run("dag-scope parsing", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			query         string
			expectedScope DagScope
			expectedError bool
		}{
			{"dag-scope=entity", DagScopeEntity, false},
			{"dag-scope=block", DagScopeBlock, false},
			{"dag-scope=all", DagScopeAll, false},
			{"dag-scope=what-is-this", "", true},
		}
		for _, test := range tests {
			r := mustNewRequest(t, http.MethodGet, "http://example.com/?"+test.query, nil)
			params, err := buildCarParams(r, map[string]string{})
			if test.expectedError {
				assert.Error(t, err)
			} else {
				assert.Equal(t, test.expectedScope, params.Scope)
			}
		}
	})

	t.Run("entity-bytes parsing", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			query    string
			hasError bool
			from     int64
			hasTo    bool
			to       int64
		}{
			{"entity-bytes=0:1000", false, 0, true, 1000},
			{"entity-bytes=1000:2000", false, 1000, true, 2000},
			{"entity-bytes=:2000", true, 0, false, 0},
			{"entity-bytes=0:*", false, 0, false, 0},
			{"entity-bytes=499:-1000", false, 499, true, -1000},
			{"entity-bytes=10000:500", true, 0, true, 0},
			{"entity-bytes=-1000:-2000", true, 0, true, 0},
			{"entity-bytes=1234:123:123", true, 0, true, 0},
			{"entity-bytes=aaa:123", true, 0, true, 0},
			{"entity-bytes=123:bbb", true, 0, true, 0},
		}
		for _, test := range tests {
			r := mustNewRequest(t, http.MethodGet, "http://example.com/?"+test.query, nil)
			params, err := buildCarParams(r, map[string]string{})
			if test.hasError {
				assert.Error(t, err)
			} else {
				assert.Equal(t, test.from, params.Range.From)
				if test.hasTo {
					assert.Equal(t, test.to, *params.Range.To)
				} else {
					assert.Nil(t, params.Range.To)
				}
			}
		}
	})

	t.Run("buildCarParams from Accept header: order and dups parsing", func(t *testing.T) {
		t.Parallel()

		// below ensure the implicit default (DFS and no duplicates) is correctly inferred
		// from the value read from Accept header
		tests := []struct {
			acceptHeader       string
			params             url.Values
			expectedOrder      DagOrder
			expectedDuplicates DuplicateBlocksPolicy
		}{
			{"application/vnd.ipld.car; order=dfs; dups=y", nil, DagOrderDFS, DuplicateBlocksIncluded},
			{"application/vnd.ipld.car; order=unk; dups=n", nil, DagOrderUnknown, DuplicateBlocksExcluded},
			{"application/vnd.ipld.car; order=unk", nil, DagOrderUnknown, DuplicateBlocksExcluded},
			{"application/vnd.ipld.car; dups=y", nil, DagOrderDFS, DuplicateBlocksIncluded},
			{"application/vnd.ipld.car; dups=n", nil, DagOrderDFS, DuplicateBlocksExcluded},
			{"application/vnd.ipld.car", nil, DagOrderDFS, DuplicateBlocksExcluded},
			{"application/vnd.ipld.car;version=1;order=dfs;dups=y", nil, DagOrderDFS, DuplicateBlocksIncluded},
			{"application/vnd.ipld.car;version=1;order=dfs;dups=y", url.Values{"car-order": []string{"unk"}}, DagOrderDFS, DuplicateBlocksIncluded},
			{"application/vnd.ipld.car;version=1;dups=y", url.Values{"car-order": []string{"unk"}}, DagOrderUnknown, DuplicateBlocksIncluded},
		}
		for _, test := range tests {
			r := mustNewRequest(t, http.MethodGet, "http://example.com/?"+test.params.Encode(), nil)
			r.Header.Set("Accept", test.acceptHeader)

			mediaType, formatParams, err := customResponseFormat(r)
			assert.NoError(t, err)
			assert.Equal(t, carResponseFormat, mediaType)

			params, err := buildCarParams(r, formatParams)
			assert.NoError(t, err)

			// order from IPIP-412
			require.Equal(t, test.expectedOrder, params.Order)

			// dups from IPIP-412
			require.Equal(t, test.expectedDuplicates.String(), params.Duplicates.String())
		}
	})

	t.Run("buildCarParams from Accept header: order and dups parsing (invalid)", func(t *testing.T) {
		t.Parallel()

		// below ensure the implicit default (DFS and no duplicates) is correctly inferred
		// from the value read from Accept header
		tests := []string{
			"application/vnd.ipld.car; dups=invalid",
			"application/vnd.ipld.car; order=invalid",
			"application/vnd.ipld.car; order=dfs; dups=invalid",
			"application/vnd.ipld.car; order=invalid; dups=y",
		}
		for _, test := range tests {
			r := mustNewRequest(t, http.MethodGet, "http://example.com/", nil)
			r.Header.Set("Accept", test)

			mediaType, formatParams, err := customResponseFormat(r)
			assert.NoError(t, err)
			assert.Equal(t, carResponseFormat, mediaType)

			_, err = buildCarParams(r, formatParams)
			assert.ErrorContains(t, err, "unsupported application/vnd.ipld.car content type")
		}
	})
}

func TestContentTypeFromCarParams(t *testing.T) {
	t.Parallel()

	// below ensures buildContentTypeFromCarParams produces correct Content-Type
	// at this point we do not do any inferring, it happens in buildCarParams instead
	// and tests of *Unspecified here are just present for completes and to guard
	// against regressions between refactors
	tests := []struct {
		params CarParams
		header string
	}{
		{CarParams{}, "application/vnd.ipld.car; version=1"},
		{CarParams{Order: DagOrderUnspecified, Duplicates: DuplicateBlocksUnspecified}, "application/vnd.ipld.car; version=1"},
		{CarParams{Order: DagOrderDFS, Duplicates: DuplicateBlocksIncluded}, "application/vnd.ipld.car; version=1; order=dfs; dups=y"},
		{CarParams{Order: DagOrderUnknown, Duplicates: DuplicateBlocksIncluded}, "application/vnd.ipld.car; version=1; order=unk; dups=y"},
		{CarParams{Order: DagOrderUnknown}, "application/vnd.ipld.car; version=1; order=unk"},
		{CarParams{Duplicates: DuplicateBlocksIncluded}, "application/vnd.ipld.car; version=1; dups=y"},
		{CarParams{Duplicates: DuplicateBlocksExcluded}, "application/vnd.ipld.car; version=1; dups=n"},
	}
	for _, test := range tests {
		header := buildContentTypeFromCarParams(test.params)
		assert.Equal(t, test.header, header)
	}
}

func TestGetCarEtag(t *testing.T) {
	t.Parallel()

	cid, err := cid.Parse("bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	require.NoError(t, err)

	imPath, err := path.NewImmutablePath(path.FromCid(cid))
	require.NoError(t, err)

	t.Run("Etag with entity-bytes=0:* is the same as without query param", func(t *testing.T) {
		t.Parallel()

		noRange := getCarEtag(imPath, CarParams{}, cid)
		withRange := getCarEtag(imPath, CarParams{Range: &DagByteRange{From: 0}}, cid)
		require.Equal(t, noRange, withRange)
	})

	t.Run("Etag with entity-bytes=1:* is different than without query param", func(t *testing.T) {
		t.Parallel()

		noRange := getCarEtag(imPath, CarParams{}, cid)
		withRange := getCarEtag(imPath, CarParams{Range: &DagByteRange{From: 1}}, cid)
		require.NotEqual(t, noRange, withRange)
	})

	t.Run("Etags with different dag-scope are different", func(t *testing.T) {
		t.Parallel()

		a := getCarEtag(imPath, CarParams{Scope: DagScopeAll}, cid)
		b := getCarEtag(imPath, CarParams{Scope: DagScopeEntity}, cid)
		require.NotEqual(t, a, b)
	})
}
