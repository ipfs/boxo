package types

import (
	"encoding/json"

	"github.com/multiformats/go-multiaddr"
)

type Multiaddr struct{ multiaddr.Multiaddr }

func (m *Multiaddr) UnmarshalJSON(b []byte) error {
	var s string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}
	ma, err := multiaddr.NewMultiaddr(s)
	if err != nil {
		return err
	}
	m.Multiaddr = ma
	return nil
}
