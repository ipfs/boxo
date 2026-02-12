package types

import (
	"encoding/json"
	"fmt"
	"net"
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
func NewAddress(s string) (Address, error) {
	addr := Address{raw: s}

	// IPIP-518 parsing logic
	if strings.HasPrefix(s, "/") {
		// Parse as multiaddr
		ma, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			return Address{}, fmt.Errorf("invalid multiaddr: %w", err)
		}
		addr.multiaddr = ma
	} else {
		// Parse as URI - accept any valid URI scheme
		u, err := url.Parse(s)
		if err != nil {
			return Address{}, fmt.Errorf("invalid uri: %w", err)
		}
		// Must be absolute URL
		if !u.IsAbs() {
			return Address{}, fmt.Errorf("uri must be absolute")
		}
		addr.url = u
	}

	return addr, nil
}

// NewAddressFromMultiaddr creates a new Address from a multiaddr.
func NewAddressFromMultiaddr(ma multiaddr.Multiaddr) Address {
	return Address{
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

	*a = addr
	return nil
}

// Protocols returns the protocols in this address.
// For multiaddrs, it returns the multiaddr protocols.
// For URLs, it returns the scheme.
func (a *Address) Protocols() []string {
	if a.url != nil {
		return []string{a.url.Scheme}
	}
	if a.multiaddr != nil {
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
// Special handling for http/https as per IPIP-518:
// - "http" matches http://, https:// URLs and /http, /tls/http multiaddrs
// - "https" matches https:// URLs, /tls/http, /https multiaddrs
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
		default:
			return scheme == proto
		}
	}

	if a.multiaddr != nil {
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
				switch p {
				case "https":
					return true
				case "tls":
					hasTLS = true
				case "http":
					hasHTTP = true
				}
			}
			return hasTLS && hasHTTP
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

// ToMultiaddr attempts to convert an HTTP(S) URL to a multiaddr for backward compatibility.
// Returns nil if the address cannot be converted (e.g., non-HTTP schemes or invalid addresses).
// This is a temporary compatibility layer for the transition period while existing software
// expects multiaddrs with /http protocol to signal HTTP retrieval support.
func (a *Address) ToMultiaddr() multiaddr.Multiaddr {
	// If already a multiaddr, return it as-is
	if a.IsMultiaddr() {
		return a.multiaddr
	}

	// If not a URL, cannot convert
	if !a.IsURL() || a.url == nil {
		return nil
	}

	// Only convert http/https URLs
	scheme := strings.ToLower(a.url.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil
	}

	// Parse hostname and port
	host := a.url.Hostname()
	port := a.url.Port()

	// Set default ports if not specified
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}

	// Determine address type
	var addrProto string
	if ip := net.ParseIP(host); ip != nil {
		// Use IP-specific protocols for IP addresses
		if ip.To4() != nil {
			addrProto = "ip4"
		} else {
			addrProto = "ip6"
		}
	} else {
		// Use generic /dns for domain names (resolves to both IPv4 and IPv6)
		addrProto = "dns"
	}

	// Build multiaddr string using scheme directly (http or https)
	maStr := fmt.Sprintf("/%s/%s/tcp/%s/%s", addrProto, host, port, scheme)

	// Preserve URL path as http-path component.
	// Leading "/" is stripped per examples in the http-path spec:
	// https://github.com/multiformats/multiaddr/blob/master/protocols/http-path.md
	if p := strings.TrimPrefix(a.url.Path, "/"); p != "" {
		maStr += "/http-path/" + url.PathEscape(p)
	}

	// Create and return multiaddr
	ma, err := multiaddr.NewMultiaddr(maStr)
	if err != nil {
		// Log the error for debugging but return nil
		// This can happen with invalid hostnames or other edge cases
		return nil
	}
	return ma
}

// Addresses is a slice of Address that can be marshaled/unmarshaled from/to JSON.
type Addresses []Address

// String returns a string representation of the addresses for printing.
func (addrs Addresses) String() string {
	if len(addrs) == 0 {
		return "[]"
	}

	strs := make([]string, len(addrs))
	for i, addr := range addrs {
		strs[i] = addr.String()
	}
	return fmt.Sprintf("[%s]", strings.Join(strs, " "))
}

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
			addr = a
		}
		// Always add the address, even if invalid (will be skipped during filtering)
		result = append(result, addr)
	}

	*addrs = result
	return nil
}
