package drjson

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarshalJSON(t *testing.T) {
	// ensure that < is not escaped, which is the default Go behavior
	bytes, err := MarshalJSONBytes(map[string]string{"<": "<"})
	if err != nil {
		panic(err)
	}
	require.Equal(t, "{\"<\":\"<\"}", string(bytes))
}
