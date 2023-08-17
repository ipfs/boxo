package iter

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONIter(t *testing.T) {
	type obj struct {
		A int
	}

	type expResult struct {
		val         obj
		errContains string
	}

	for _, c := range []struct {
		jsonStr    string
		expResults []expResult
	}{
		{
			jsonStr: "{\"a\":1}\n{\"a\":2}",
			expResults: []expResult{
				{val: obj{A: 1}},
				{val: obj{A: 2}},
			},
		},
		{
			jsonStr:    "",
			expResults: nil,
		},
		{
			jsonStr:    "\n",
			expResults: nil,
		},
		{
			jsonStr: "{\"a\":1}{\"a\":2}",
			expResults: []expResult{
				{val: obj{A: 1}},
				{val: obj{A: 2}},
			},
		},
		{
			jsonStr: "{\"a\":1}{\"a\":asdf}",
			expResults: []expResult{
				{val: obj{A: 1}},
				{errContains: "invalid character"},
			},
		},
	} {
		t.Run(c.jsonStr, func(t *testing.T) {
			reader := bytes.NewReader([]byte(c.jsonStr))
			iter := FromReaderJSON[obj](reader)
			results := ReadAll[Result[obj]](iter)

			require.Len(t, results, len(c.expResults))
			for i, res := range results {
				expRes := c.expResults[i]
				if expRes.errContains != "" {
					assert.ErrorContains(t, res.Err, expRes.errContains)
				} else {
					assert.NoError(t, res.Err)
					assert.Equal(t, expRes.val, res.Val)
				}
			}
		})
	}
}
