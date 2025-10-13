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
			wantErr:     false,
			isURL:       false,
			isMultiaddr: true,
			protocols:   []string{"ip4", "tcp"},
		},
		{
			name:        "valid https URL",
			input:       "https://example.com",
			wantErr:     false,
			isURL:       true,
			isMultiaddr: false,
			protocols:   []string{"https"},
		},
		{
			name:        "valid http URL",
			input:       "http://example.com:8080",
			wantErr:     false,
			isURL:       true,
			isMultiaddr: false,
			protocols:   []string{"http"},
		},
		{
			name:        "valid http URL with path",
			input:       "http://example.com:8080/path",
			wantErr:     false,
			isURL:       true,
			isMultiaddr: false,
			protocols:   []string{"http"},
		},
		{
			name:        "other URI scheme foo",
			input:       "foo://example.com/path",
			wantErr:     false,
			isURL:       true,
			isMultiaddr: false,
			protocols:   []string{"foo"},
		},
		{
			name:        "other URI scheme bar",
			input:       "bar://something",
			wantErr:     false,
			isURL:       true,
			isMultiaddr: false,
			protocols:   []string{"bar"},
		},
		{
			name:        "relative URL",
			input:       "example.com",
			wantErr:     true,
			isURL:       false,
			isMultiaddr: false,
		},
		{
			name:        "invalid multiaddr",
			input:       "/invalid",
			wantErr:     true,
			isURL:       false,
			isMultiaddr: false,
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
			name:     "https URL matches tls",
			address:  "https://example.com",
			protocol: "tls",
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
		{
			name:     "http URL doesn't match tls",
			address:  "http://example.com",
			protocol: "tls",
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
			data, err := json.Marshal(addr)
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

func TestPeerRecordWithMixedAddresses(t *testing.T) {
	// Test that PeerRecord can handle mixed addresses
	jsonData := `{
		"Schema": "peer",
		"ID": "12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn",
		"Addrs": [
			"/ip4/192.168.1.1/tcp/4001",
			"https://trustless-gateway.example.com",
			"http://example.org:8080",
			"/dns4/libp2p.example.com/tcp/443/wss"
		],
		"Protocols": ["transport-bitswap", "transport-ipfs-gateway-http"]
	}`

	var pr PeerRecord
	err := json.Unmarshal([]byte(jsonData), &pr)
	require.NoError(t, err)

	assert.Equal(t, "peer", pr.Schema)
	assert.Len(t, pr.Addrs, 4)

	// Check each address
	assert.True(t, pr.Addrs[0].IsMultiaddr())
	assert.True(t, pr.Addrs[1].IsURL())
	assert.True(t, pr.Addrs[2].IsURL())
	assert.True(t, pr.Addrs[3].IsMultiaddr())

	// Marshal back
	data, err := json.Marshal(pr)
	require.NoError(t, err)

	// Check it's valid JSON
	var check map[string]interface{}
	err = json.Unmarshal(data, &check)
	require.NoError(t, err)

	addrs, ok := check["Addrs"].([]interface{})
	require.True(t, ok)
	assert.Len(t, addrs, 4)
}
