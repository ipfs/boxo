package drjson

import (
	"bytes"
	"encoding/json"
)

func marshalJSON(val any) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	err := enc.Encode(val)
	return buf.Bytes(), err
}

// MarshalJSONBytes is needed to avoid changes
// on the original bytes due to HTML escapes.
func MarshalJSONBytes(val any) ([]byte, error) {
	bytes, err := marshalJSON(val)
	if err != nil {
		return nil, err
	}

	// remove last \n added by Encode
	return bytes[:len(bytes)-1], nil
}
