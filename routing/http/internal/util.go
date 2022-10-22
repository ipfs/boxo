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
