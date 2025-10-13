package types

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/ipfs/boxo/routing/http/internal/drjson"
	"github.com/multiformats/go-multiaddr"
)

// Address represents an address that can be either a multiaddr or a URI.
// It implements the parsing logic from IPIP-518: strings starting with '/'
// are parsed as multiaddrs, others are parsed as URIs.
// This type is schema-agnostic and will accept any valid URI scheme.
type Address struct {
	raw       string
	multiaddr multiaddr.Multiaddr
	url       *url.URL
}

// NewAddress creates a new Address from a string.
// It accepts any valid multiaddr or URI, following IPIP-518 parsing rules.
func NewAddress(s string) (*Address, error) {
	addr := &Address{raw: s}

	// IPIP-518 parsing logic
	if strings.HasPrefix(s, "/") {
		// Parse as multiaddr
		ma, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			return nil, fmt.Errorf("invalid multiaddr: %w", err)
		}
		addr.multiaddr = ma
	} else {
		// Parse as URI - accept any valid URI scheme
		u, err := url.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("invalid URI: %w", err)
		}
		// Must be absolute URL
		if !u.IsAbs() {
			return nil, fmt.Errorf("URI must be absolute")
		}
		addr.url = u
	}

	return addr, nil
}

// NewAddressFromMultiaddr creates a new Address from a multiaddr.
func NewAddressFromMultiaddr(ma multiaddr.Multiaddr) *Address {
	return &Address{
		raw:       ma.String(),
		multiaddr: ma,
	}
}

// String returns the original string representation of the address.
func (a *Address) String() string {
	return a.raw
}

// Multiaddr returns the multiaddr if this is a multiaddr, nil otherwise.
func (a *Address) Multiaddr() multiaddr.Multiaddr {
	return a.multiaddr
}

// URL returns the URL if this is a URL, nil otherwise.
func (a *Address) URL() *url.URL {
	return a.url
}

// IsMultiaddr returns true if this address is a multiaddr.
func (a *Address) IsMultiaddr() bool {
	return a.multiaddr != nil
}

// IsURL returns true if this address is a URL.
func (a *Address) IsURL() bool {
	return a.url != nil
}

// IsValid returns true if the address was successfully parsed as either
// a multiaddr or a URI. Returns false for unparseable addresses.
func (a *Address) IsValid() bool {
	return a.multiaddr != nil || a.url != nil
}

// MarshalJSON implements json.Marshaler.
func (a *Address) MarshalJSON() ([]byte, error) {
	return drjson.MarshalJSONBytes(a.raw)
}

// UnmarshalJSON implements json.Unmarshaler.
func (a *Address) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	addr, err := NewAddress(s)
	if err != nil {
		// Per IPIP-518: implementations MUST skip addresses they cannot parse
		// We'll store the raw string but mark it as invalid
		a.raw = s
		a.multiaddr = nil
		a.url = nil
		return nil // Don't return error, just skip
	}

	*a = *addr
	return nil
}

// Protocols returns the protocols in this address.
// For multiaddrs, it returns the multiaddr protocols.
// For URLs, it returns the scheme.
func (a *Address) Protocols() []string {
	if a.url != nil {
		return []string{a.url.Scheme}
	} else if a.multiaddr != nil {
		protos := a.multiaddr.Protocols()
		result := make([]string, len(protos))
		for i, p := range protos {
			result[i] = p.Name
		}
		return result
	}
	return nil
}

// HasProtocol checks if the address contains the given protocol.
// For URLs, it checks the scheme. For multiaddrs, it checks the protocols.
// Special handling for http/https/tls as per IPIP-518:
// - "http" matches http://, https:// URLs and /http, /tls/http multiaddrs
// - "https" matches https:// URLs, /tls/http, /https multiaddrs
// - "tls" matches /tls multiaddrs AND https:// URLs
func (a *Address) HasProtocol(proto string) bool {
	proto = strings.ToLower(proto)

	if a.url != nil {
		scheme := strings.ToLower(a.url.Scheme)

		switch proto {
		case "http":
			// "http" matches both http and https URLs
			return scheme == "http" || scheme == "https"
		case "https":
			return scheme == "https"
		case "tls":
			// TLS matches https URLs
			return scheme == "https"
		default:
			return scheme == proto
		}
	} else if a.multiaddr != nil {
		protocols := a.Protocols()

		switch proto {
		case "http":
			// "http" matches /http, including /tls/http
			for _, p := range protocols {
				if p == "http" {
					return true
				}
			}
		case "https":
			// "https" matches /https or the combination /tls/http
			hasTLS := false
			hasHTTP := false
			for _, p := range protocols {
				if p == "https" {
					return true
				}
				if p == "tls" {
					hasTLS = true
				}
				if p == "http" {
					hasHTTP = true
				}
			}
			return hasTLS && hasHTTP
		case "tls":
			// "tls" matches any multiaddr with /tls
			for _, p := range protocols {
				if p == "tls" {
					return true
				}
			}
		default:
			for _, p := range protocols {
				if p == proto {
					return true
				}
			}
		}
	}

	return false
}

// Addresses is a slice of Address that can be marshaled/unmarshaled from/to JSON.
type Addresses []Address

// MarshalJSON implements json.Marshaler for Addresses.
func (addrs Addresses) MarshalJSON() ([]byte, error) {
	strs := make([]string, len(addrs))
	for i, addr := range addrs {
		strs[i] = addr.String()
	}
	return json.Marshal(strs)
}

// UnmarshalJSON implements json.Unmarshaler for Addresses.
// Per IPIP-518, it MUST skip addresses that cannot be parsed.
func (addrs *Addresses) UnmarshalJSON(b []byte) error {
	var strs []string
	if err := json.Unmarshal(b, &strs); err != nil {
		return err
	}

	result := make(Addresses, 0, len(strs))
	for _, s := range strs {
		addr := Address{raw: s}
		if a, err := NewAddress(s); err == nil {
			addr = *a
		}
		// Always add the address, even if invalid (will be skipped during filtering)
		result = append(result, addr)
	}

	*addrs = result
	return nil
}
