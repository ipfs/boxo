package gateway

import (
	"net/http"
	"testing"

	"github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
)

func TestCarParams(t *testing.T) {
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
			r, err := http.NewRequest(http.MethodGet, "http://example.com/?"+test.query, nil)
			assert.NoError(t, err)

			params, err := getCarParams(r)

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
			r, err := http.NewRequest(http.MethodGet, "http://example.com/?"+test.query, nil)
			assert.NoError(t, err)

			params, err := getCarParams(r)

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
}

func TestCarEtag(t *testing.T) {
	cid, err := cid.Parse("bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	assert.NoError(t, err)

	imPath, err := NewImmutablePath(path.IpfsPath(cid))
	assert.NoError(t, err)

	t.Run("Etag with entity-bytes=0:* is the same as without query param", func(t *testing.T) {
		t.Parallel()

		noRange := getCarEtag(imPath, CarParams{}, cid)
		withRange := getCarEtag(imPath, CarParams{Range: &DagByteRange{From: 0}}, cid)
		assert.Equal(t, noRange, withRange)
	})

	t.Run("Etag with entity-bytes=1:* is different than without query param", func(t *testing.T) {
		t.Parallel()

		noRange := getCarEtag(imPath, CarParams{}, cid)
		withRange := getCarEtag(imPath, CarParams{Range: &DagByteRange{From: 1}}, cid)
		assert.NotEqual(t, noRange, withRange)
	})

	t.Run("Etags with different dag-scope are different", func(t *testing.T) {
		t.Parallel()

		a := getCarEtag(imPath, CarParams{Scope: DagScopeAll}, cid)
		b := getCarEtag(imPath, CarParams{Scope: DagScopeEntity}, cid)
		assert.NotEqual(t, a, b)
	})
}
