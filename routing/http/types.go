package delegatedrouting

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
)

type Time struct{ time.Time }

func (t *Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time.UnixMilli())
}
func (t *Time) UnmarshalJSON(b []byte) error {
	var timestamp int64
	err := json.Unmarshal(b, &timestamp)
	if err != nil {
		return err
	}
	t.Time = time.UnixMilli(timestamp)
	return nil
}

type Duration struct{ time.Duration }

func (d *Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.Duration) }
func (d *Duration) UnmarshalJSON(b []byte) error {
	var dur int64
	err := json.Unmarshal(b, &dur)
	if err != nil {
		return err
	}
	d.Duration = time.Duration(dur) * time.Millisecond
	return nil
}

type CID struct{ cid.Cid }

func (c *CID) MarshalJSON() ([]byte, error) { return json.Marshal(c.String()) }
func (c *CID) UnmarshalJSON(b []byte) error {
	var s string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}
	decodedCID, err := cid.Decode(s)
	if err != nil {
		return err
	}
	c.Cid = decodedCID
	return nil
}

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

type Provider interface{}
type WriteProviderResponse interface{}

type UnknownWriteProviderResponse struct {
	Bytes []byte
}

func (r *UnknownWriteProviderResponse) UnmarshalJSON(b []byte) error {
	r.Bytes = b
	return nil
}

func (r UnknownWriteProviderResponse) MarshalJSON() ([]byte, error) {
	// the response type must be an object
	m := map[string]interface{}{}
	err := json.Unmarshal(r.Bytes, &m)
	if err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

type WriteProvidersRequest struct {
	Providers []Provider
}

func (r *WriteProvidersRequest) UnmarshalJSON(b []byte) error {
	type wpr struct {
		Providers []json.RawMessage
	}
	var tempWPR wpr
	err := json.Unmarshal(b, &tempWPR)
	if err != nil {
		return err
	}

	for _, provBytes := range tempWPR.Providers {
		var rawProv RawProvider
		err := json.Unmarshal(provBytes, &rawProv)
		if err != nil {
			return err
		}

		switch rawProv.Protocol {
		case "bitswap":
			var prov BitswapWriteProviderRequest
			err := json.Unmarshal(rawProv.bytes, &prov)
			if err != nil {
				return err
			}
			r.Providers = append(r.Providers, &prov)
		default:
			var prov UnknownProvider
			err := json.Unmarshal(b, &prov)
			if err != nil {
				return err
			}
			r.Providers = append(r.Providers, &prov)
		}
	}
	return nil
}

type WriteProvidersResponse struct {
	// Protocols is the list of protocols expected for each result.
	// This is required to unmarshal the result types.
	// It can be derived from the request that was sent.
	// If this is nil, then each WriteProviderResponse will contain a map[string]interface{}.
	Protocols []string `json:"-"`

	ProvideResults []WriteProviderResponse
}

func (r *WriteProvidersResponse) UnmarshalJSON(b []byte) error {
	type wpr struct {
		ProvideResults []json.RawMessage
	}
	var tempWPR wpr
	err := json.Unmarshal(b, &tempWPR)
	if err != nil {
		return err
	}
	if r.Protocols != nil && len(r.Protocols) != len(tempWPR.ProvideResults) {
		return fmt.Errorf("got %d results but only have protocol information for %d", len(tempWPR.ProvideResults), len(r.Protocols))
	}

	r.ProvideResults = make([]WriteProviderResponse, len(r.Protocols))

	for i, provResBytes := range tempWPR.ProvideResults {
		if r.Protocols == nil {
			m := map[string]interface{}{}
			err := json.Unmarshal(provResBytes, &m)
			if err != nil {
				return fmt.Errorf("error unmarshaling element %d of response: %w", len(tempWPR.ProvideResults), err)
			}
			r.ProvideResults[i] = m
			continue
		}

		var val any
		switch r.Protocols[i] {
		case "bitswap":
			var resp BitswapWriteProviderResponse
			err := json.Unmarshal(provResBytes, &resp)
			if err != nil {
				return err
			}
			val = &resp
		default:
			val = &UnknownWriteProviderResponse{Bytes: provResBytes}
		}

		r.ProvideResults[i] = val
	}
	return nil
}

type RawProvider struct {
	Protocol string
	bytes    []byte
}

func (p *RawProvider) UnmarshalJSON(b []byte) error {
	v := struct{ Protocol string }{}
	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}
	p.bytes = b
	p.Protocol = v.Protocol
	return nil
}

func (p *RawProvider) MarshalJSON() ([]byte, error) {
	return p.bytes, nil
}

type UnknownProvider struct {
	Protocol string
	Bytes    []byte
}

type FindProvidersResponse struct {
	Providers []Provider
}

func (r *FindProvidersResponse) UnmarshalJSON(b []byte) error {
	type fpr struct {
		Providers []json.RawMessage
	}
	var tempFPR fpr
	err := json.Unmarshal(b, &tempFPR)
	if err != nil {
		return err
	}

	for _, provBytes := range tempFPR.Providers {
		var readProv RawProvider
		err := json.Unmarshal(provBytes, &readProv)
		if err != nil {
			return err
		}

		switch readProv.Protocol {
		case "bitswap":
			var prov BitswapReadProviderResponse
			err := json.Unmarshal(readProv.bytes, &prov)
			if err != nil {
				return err
			}
			r.Providers = append(r.Providers, &prov)
		default:
			var prov UnknownProvider
			err := json.Unmarshal(b, &prov)
			if err != nil {
				return err
			}
			r.Providers = append(r.Providers, &prov)
		}

	}
	return nil
}

func (u *UnknownProvider) UnmarshalJSON(b []byte) error {
	u.Bytes = b
	return nil
}

func (u UnknownProvider) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{}
	err := json.Unmarshal(u.Bytes, &m)
	if err != nil {
		return nil, err
	}
	m["Protocol"] = u.Protocol

	return json.Marshal(m)
}
