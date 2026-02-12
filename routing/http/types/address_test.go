package types

import (
	"encoding/json"
	"testing"

	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAddress(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantErr     bool
		isURL       bool
		isMultiaddr bool
		protocols   []string
	}{
		{
			name:        "valid multiaddr",
			input:       "/ip4/127.0.0.1/tcp/4001",
			isMultiaddr: true,
			protocols:   []string{"ip4", "tcp"},
		},
		{
			name:      "valid https URL",
			input:     "https://example.com",
			isURL:     true,
			protocols: []string{"https"},
		},
		{
			name:      "valid http URL",
			input:     "http://example.com:8080",
			isURL:     true,
			protocols: []string{"http"},
		},
		{
			name:      "valid http URL with path",
			input:     "http://example.com:8080/path",
			isURL:     true,
			protocols: []string{"http"},
		},
		{
			name:      "other URI scheme foo",
			input:     "foo://example.com/path",
			isURL:     true,
			protocols: []string{"foo"},
		},
		{
			name:      "other URI scheme bar",
			input:     "bar://something",
			isURL:     true,
			protocols: []string{"bar"},
		},
		{
			name:    "relative URL",
			input:   "example.com",
			wantErr: true,
		},
		{
			name:    "invalid multiaddr",
			input:   "/invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, err := NewAddress(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.input, addr.String())
			assert.Equal(t, tt.isURL, addr.IsURL())
			assert.Equal(t, tt.isMultiaddr, addr.IsMultiaddr())
			assert.Equal(t, tt.protocols, addr.Protocols())
		})
	}
}

func TestAddressHasProtocol(t *testing.T) {
	tests := []struct {
		name     string
		address  string
		protocol string
		expected bool
	}{
		// Multiaddr tests
		{
			name:     "multiaddr has tcp",
			address:  "/ip4/127.0.0.1/tcp/4001",
			protocol: "tcp",
			expected: true,
		},
		{
			name:     "multiaddr doesn't have udp",
			address:  "/ip4/127.0.0.1/tcp/4001",
			protocol: "udp",
			expected: false,
		},
		{
			name:     "multiaddr with /http",
			address:  "/dns4/example.com/tcp/80/http",
			protocol: "http",
			expected: true,
		},
		{
			name:     "multiaddr with /tls/http matches https",
			address:  "/dns4/example.com/tcp/443/tls/http",
			protocol: "https",
			expected: true,
		},
		{
			name:     "multiaddr with /tls/http matches http",
			address:  "/dns4/example.com/tcp/443/tls/http",
			protocol: "http",
			expected: true,
		},
		{
			name:     "multiaddr with /tls/http matches tls",
			address:  "/dns4/example.com/tcp/443/tls/http",
			protocol: "tls",
			expected: true,
		},
		// URL tests
		{
			name:     "https URL matches https",
			address:  "https://example.com",
			protocol: "https",
			expected: true,
		},
		{
			name:     "https URL matches http",
			address:  "https://example.com",
			protocol: "http",
			expected: true,
		},
		{
			name:     "http URL matches http",
			address:  "http://example.com",
			protocol: "http",
			expected: true,
		},
		{
			name:     "http URL doesn't match https",
			address:  "http://example.com",
			protocol: "https",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, err := NewAddress(tt.address)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, addr.HasProtocol(tt.protocol))
		})
	}
}

func TestAddressJSON(t *testing.T) {
	tests := []struct {
		name    string
		address string
	}{
		{
			name:    "multiaddr",
			address: "/ip4/127.0.0.1/tcp/4001",
		},
		{
			name:    "https URL",
			address: "https://example.com",
		},
		{
			name:    "http URL with port",
			address: "http://example.com:8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, err := NewAddress(tt.address)
			require.NoError(t, err)

			// Marshal to JSON
			data, err := json.Marshal(&addr)
			require.NoError(t, err)

			// Should be a JSON string
			var str string
			err = json.Unmarshal(data, &str)
			require.NoError(t, err)
			assert.Equal(t, tt.address, str)

			// Unmarshal back
			var addr2 Address
			err = json.Unmarshal(data, &addr2)
			require.NoError(t, err)
			assert.Equal(t, addr.String(), addr2.String())
			assert.Equal(t, addr.IsURL(), addr2.IsURL())
			assert.Equal(t, addr.IsMultiaddr(), addr2.IsMultiaddr())
		})
	}
}

func TestAddressesJSON(t *testing.T) {
	input := []string{
		"/ip4/127.0.0.1/tcp/4001",
		"https://example.com",
		"http://localhost:8080",
		"/invalid/addr", // This should be included but marked as invalid
	}

	// Create Addresses from strings
	var addrs Addresses
	data, err := json.Marshal(input)
	require.NoError(t, err)

	err = json.Unmarshal(data, &addrs)
	require.NoError(t, err)

	// Should have all 4 addresses
	assert.Len(t, addrs, 4)

	// First three should be valid
	assert.True(t, addrs[0].IsValid())
	assert.True(t, addrs[0].IsMultiaddr())

	assert.True(t, addrs[1].IsValid())
	assert.True(t, addrs[1].IsURL())

	assert.True(t, addrs[2].IsValid())
	assert.True(t, addrs[2].IsURL())

	// Last one should be invalid but present
	assert.False(t, addrs[3].IsValid())
	assert.Equal(t, "/invalid/addr", addrs[3].String())

	// Marshal back should give the same strings
	data2, err := json.Marshal(addrs)
	require.NoError(t, err)

	var output []string
	err = json.Unmarshal(data2, &output)
	require.NoError(t, err)

	assert.Equal(t, input, output)
}

func TestNewAddressFromMultiaddr(t *testing.T) {
	ma, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
	require.NoError(t, err)

	addr := NewAddressFromMultiaddr(ma)
	assert.True(t, addr.IsMultiaddr())
	assert.False(t, addr.IsURL())
	assert.Equal(t, ma.String(), addr.String())
	assert.Equal(t, ma, addr.Multiaddr())
	assert.Nil(t, addr.URL())
}

func TestPeerRecordMultiaddrsOnly(t *testing.T) {
	// PeerRecord Addrs field is []Multiaddr (multiaddrs only, no URLs)
	jsonData := `{
		"Schema": "peer",
		"ID": "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn",
		"Addrs": [
			"/ip4/192.168.1.1/tcp/4001",
			"/dns4/libp2p.example.com/tcp/443/wss"
		],
		"Protocols": ["transport-bitswap"]
	}`

	var pr PeerRecord
	err := json.Unmarshal([]byte(jsonData), &pr)
	require.NoError(t, err)

	assert.Equal(t, "peer", pr.Schema)
	assert.Len(t, pr.Addrs, 2)
	assert.Equal(t, "/ip4/192.168.1.1/tcp/4001", pr.Addrs[0].String())
	assert.Equal(t, "/dns4/libp2p.example.com/tcp/443/wss", pr.Addrs[1].String())

	// Marshal back
	data, err := json.Marshal(pr)
	require.NoError(t, err)

	var check map[string]interface{}
	err = json.Unmarshal(data, &check)
	require.NoError(t, err)

	addrs, ok := check["Addrs"].([]interface{})
	require.True(t, ok)
	assert.Len(t, addrs, 2)
}

func TestPeerRecordURLsInAddrs(t *testing.T) {
	// PeerRecord.Addrs is []Multiaddr. The Multiaddr UnmarshalJSON returns
	// an error for non-multiaddr strings, so a PeerRecord with a URL in
	// Addrs fails to deserialize entirely. This documents why URLs belong
	// in GenericRecord (which uses Addresses), not PeerRecord.
	jsonData := `{
		"Schema": "peer",
		"ID": "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn",
		"Addrs": [
			"/ip4/192.168.1.1/tcp/4001",
			"https://example.com",
			"/dns4/libp2p.example.com/tcp/443/wss"
		],
		"Protocols": ["transport-bitswap"]
	}`

	var pr PeerRecord
	err := json.Unmarshal([]byte(jsonData), &pr)
	require.Error(t, err, "PeerRecord should fail to unmarshal when Addrs contains a URL")
	assert.Contains(t, err.Error(), "must begin with /")
}

func TestAddressToMultiaddr(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string // Expected multiaddr string, empty if conversion not possible
	}{
		{
			name:     "https URL with default port",
			input:    "https://example.com",
			expected: "/dns/example.com/tcp/443/https",
		},
		{
			name:     "http URL with default port",
			input:    "http://example.com",
			expected: "/dns/example.com/tcp/80/http",
		},
		{
			name:     "http URL with custom port",
			input:    "http://example.com:8080",
			expected: "/dns/example.com/tcp/8080/http",
		},
		{
			name:     "https URL with custom port",
			input:    "https://example.com:8443",
			expected: "/dns/example.com/tcp/8443/https",
		},
		{
			name:     "http URL with IPv4",
			input:    "http://192.168.1.1:8080",
			expected: "/ip4/192.168.1.1/tcp/8080/http",
		},
		{
			name:     "https URL with IPv4",
			input:    "https://192.168.1.1",
			expected: "/ip4/192.168.1.1/tcp/443/https",
		},
		{
			name:     "http URL with IPv6",
			input:    "http://[::1]:8080",
			expected: "/ip6/::1/tcp/8080/http",
		},
		{
			name:     "https URL with IPv6",
			input:    "https://[::1]",
			expected: "/ip6/::1/tcp/443/https",
		},
		{
			name:     "https URL with IPv6 and custom port",
			input:    "https://[::1]:8443",
			expected: "/ip6/::1/tcp/8443/https",
		},
		{
			name:     "http URL with path preserved",
			input:    "http://example.com/path/to/resource",
			expected: "/dns/example.com/tcp/80/http/http-path/path%2Fto%2Fresource",
		},
		{
			name:     "https URL with query - path preserved, query dropped",
			input:    "https://example.com:8443/path?query=value",
			expected: "/dns/example.com/tcp/8443/https/http-path/path",
		},
		{
			name:     "non-HTTP scheme not converted",
			input:    "ftp://example.com",
			expected: "",
		},
		{
			name:     "websocket scheme not converted",
			input:    "ws://example.com",
			expected: "",
		},
		{
			name:     "existing multiaddr returned as-is",
			input:    "/ip4/127.0.0.1/tcp/4001",
			expected: "/ip4/127.0.0.1/tcp/4001",
		},
		{
			name:     "existing http multiaddr returned as-is",
			input:    "/dns/example.com/tcp/443/https",
			expected: "/dns/example.com/tcp/443/https",
		},
		{
			name:     "localhost http",
			input:    "http://localhost:8080",
			expected: "/dns/localhost/tcp/8080/http",
		},
		{
			name:     "localhost https",
			input:    "https://localhost",
			expected: "/dns/localhost/tcp/443/https",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, err := NewAddress(tt.input)
			require.NoError(t, err)

			ma := addr.ToMultiaddr()
			if tt.expected == "" {
				assert.Nil(t, ma, "Expected nil multiaddr for %s", tt.input)
			} else {
				require.NotNil(t, ma, "Expected non-nil multiaddr for %s", tt.input)
				assert.Equal(t, tt.expected, ma.String())
			}
		})
	}

	// Test with invalid address
	t.Run("invalid address returns nil", func(t *testing.T) {
		addr := &Address{raw: "invalid"}
		ma := addr.ToMultiaddr()
		assert.Nil(t, ma)
	})
}
