package internal

import (
	"bytes"
	"encoding/json"
)

func MarshalJSON(val any) (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	err := enc.Encode(val)
	return buf, err
}

func MarshalJSONBytes(val any) ([]byte, error) {
	buf, err := MarshalJSON(val)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
