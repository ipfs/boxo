package filters

import (
	"testing"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddFiltersToURL(t *testing.T) {
	testCases := []struct {
		name           string
		baseURL        string
		protocolFilter []string
		addrFilter     []string
		expected       string
	}{
		{
			name:           "No filters",
			baseURL:        "https://example.com/routing/v1/providers/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
			protocolFilter: nil,
			addrFilter:     nil,
			expected:       "https://example.com/routing/v1/providers/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
		},
		{
			name:           "Only protocol filter",
			baseURL:        "https://example.com/routing/v1/providers/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
			protocolFilter: []string{"transport-bitswap", "transport-ipfs-gateway-http"},
			addrFilter:     nil,
			expected:       "https://example.com/routing/v1/providers/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?filter-protocols=transport-bitswap,transport-ipfs-gateway-http",
		},
		{
			name:           "Only addr filter",
			baseURL:        "https://example.com/routing/v1/providers/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
			protocolFilter: nil,
			addrFilter:     []string{"ip4", "ip6"},
			expected:       "https://example.com/routing/v1/providers/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?filter-addrs=ip4,ip6",
		},
		{
			name:           "Both filters",
			baseURL:        "https://example.com/routing/v1/providers/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
			protocolFilter: []string{"transport-bitswap", "transport-graphsync-filecoinv1"},
			addrFilter:     []string{"ip4", "ip6"},
			expected:       "https://example.com/routing/v1/providers/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?filter-addrs=ip4,ip6&filter-protocols=transport-bitswap,transport-graphsync-filecoinv1",
		},
		{
			name:           "URL with existing query parameters",
			baseURL:        "https://example.com/routing/v1/providers/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?existing=param",
			protocolFilter: []string{"transport-bitswap"},
			addrFilter:     []string{"ip4"},
			expected:       "https://example.com/routing/v1/providers/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?existing=param&filter-addrs=ip4&filter-protocols=transport-bitswap",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := AddFiltersToURL(tc.baseURL, tc.protocolFilter, tc.addrFilter)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestApplyAddrFilter(t *testing.T) {
	// Create some test multiaddrs
	addr1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt")
	addr2, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/udp/4001/quic/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt")
	addr3, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001/ws/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt")
	addr4, _ := multiaddr.NewMultiaddr("/ip4/102.101.1.1/tcp/4001/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt")
	addr5, _ := multiaddr.NewMultiaddr("/ip4/102.101.1.1/udp/4001/quic-v1/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt")
	addr6, _ := multiaddr.NewMultiaddr("/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/certhash/uEiD9f05PrY82lovP4gOFonmY7sO0E7_jyovt9p2LEcAS-Q/certhash/uEiBtGJsNz-PcywwXOVzEYeQQloQiHMqDqdj18t2Fe4GTLQ/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt")
	addr7, _ := multiaddr.NewMultiaddr("/dns4/ny5.bootstrap.libp2p.io/tcp/443/wss/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt")
	addr8, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/udp/4001/quic-v1/webtransport/certhash/uEiAMrMcVWFNiqtSeRXZTwHTac4p9WcGh5hg8kVBzTC1JTA/certhash/uEiA4dfvbbbnBIYalhp1OpW1Bk-nuWIKSy21ol6vPea67Cw/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt")

	addrs := types.Addresses{
		types.NewAddressFromMultiaddr(addr1),
		types.NewAddressFromMultiaddr(addr2),
		types.NewAddressFromMultiaddr(addr3),
		types.NewAddressFromMultiaddr(addr4),
		types.NewAddressFromMultiaddr(addr5),
		types.NewAddressFromMultiaddr(addr6),
		types.NewAddressFromMultiaddr(addr7),
		types.NewAddressFromMultiaddr(addr8),
	}

	testCases := []struct {
		name          string
		filterAddrs   []string
		expectedAddrs types.Addresses
	}{
		{
			name:          "No filter",
			filterAddrs:   []string{},
			expectedAddrs: addrs,
		},
		{
			name:          "Filter TCP",
			filterAddrs:   []string{"tcp"},
			expectedAddrs: types.Addresses{types.NewAddressFromMultiaddr(addr1), types.NewAddressFromMultiaddr(addr3), types.NewAddressFromMultiaddr(addr4), types.NewAddressFromMultiaddr(addr7)},
		},
		{
			name:          "Filter UDP",
			filterAddrs:   []string{"udp"},
			expectedAddrs: types.Addresses{types.NewAddressFromMultiaddr(addr2), types.NewAddressFromMultiaddr(addr5), types.NewAddressFromMultiaddr(addr6), types.NewAddressFromMultiaddr(addr8)},
		},
		{
			name:          "Filter WebSocket",
			filterAddrs:   []string{"ws"},
			expectedAddrs: types.Addresses{types.NewAddressFromMultiaddr(addr3)},
		},
		{
			name:          "Exclude TCP",
			filterAddrs:   []string{"!tcp"},
			expectedAddrs: types.Addresses{types.NewAddressFromMultiaddr(addr2), types.NewAddressFromMultiaddr(addr5), types.NewAddressFromMultiaddr(addr6), types.NewAddressFromMultiaddr(addr8)},
		},
		{
			name:          "Filter TCP addresses that don't have WebSocket and p2p-circuit",
			filterAddrs:   []string{"tcp", "!ws", "!wss", "!p2p-circuit"},
			expectedAddrs: types.Addresses{types.NewAddressFromMultiaddr(addr1)},
		},
		{
			name:          "Include WebTransport and exclude p2p-circuit",
			filterAddrs:   []string{"webtransport", "!p2p-circuit"},
			expectedAddrs: types.Addresses{types.NewAddressFromMultiaddr(addr8)},
		},
		{
			name:          "empty for unknown protocol name",
			filterAddrs:   []string{"fakeproto"},
			expectedAddrs: types.Addresses{},
		},
		{
			name:          "Include WebTransport but ignore unknown protocol name",
			filterAddrs:   []string{"webtransport", "fakeproto"},
			expectedAddrs: types.Addresses{types.NewAddressFromMultiaddr(addr6), types.NewAddressFromMultiaddr(addr8)},
		},
		{
			name:          "Multiple filters",
			filterAddrs:   []string{"tcp", "ws"},
			expectedAddrs: types.Addresses{types.NewAddressFromMultiaddr(addr1), types.NewAddressFromMultiaddr(addr3), types.NewAddressFromMultiaddr(addr4), types.NewAddressFromMultiaddr(addr7)},
		},
		{
			name:          "Multiple negative filters",
			filterAddrs:   []string{"!tcp", "!ws"},
			expectedAddrs: types.Addresses{types.NewAddressFromMultiaddr(addr2), types.NewAddressFromMultiaddr(addr5), types.NewAddressFromMultiaddr(addr6), types.NewAddressFromMultiaddr(addr8)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := applyAddrFilter(addrs, tc.filterAddrs)
			assert.Equal(t, len(tc.expectedAddrs), len(result), "Unexpected number of addresses after filtering")

			// Check that each expected address is in the result
			for _, expectedAddr := range tc.expectedAddrs {
				found := false
				for _, resultAddr := range result {
					if expectedAddr.IsMultiaddr() && resultAddr.IsMultiaddr() && expectedAddr.Multiaddr().Equal(resultAddr.Multiaddr()) {
						found = true
						break
					}
				}
				assert.True(t, found, "Expected address not found in test %s result: %s", tc.name, expectedAddr.String())
			}

			// Check that each result address is in the expected list
			for _, resultAddr := range result {
				found := false
				for _, expectedAddr := range tc.expectedAddrs {
					if resultAddr.IsMultiaddr() && expectedAddr.IsMultiaddr() && resultAddr.Multiaddr().Equal(expectedAddr.Multiaddr()) {
						found = true
						break
					}
				}
				assert.True(t, found, "Unexpected address found in test %s result: %s", tc.name, resultAddr.String())
			}
		})
	}
}

func TestProtocolsAllowed(t *testing.T) {
	testCases := []struct {
		name            string
		peerProtocols   []string
		filterProtocols []string
		expected        bool
	}{
		{
			name:            "No filter",
			peerProtocols:   []string{"transport-bitswap", "transport-ipfs-gateway-http"},
			filterProtocols: []string{},
			expected:        true,
		},
		{
			name:            "Single matching protocol",
			peerProtocols:   []string{"transport-bitswap", "transport-ipfs-gateway-http"},
			filterProtocols: []string{"transport-bitswap"},
			expected:        true,
		},
		{
			name:            "Single non-matching protocol",
			peerProtocols:   []string{"transport-bitswap", "transport-ipfs-gateway-http"},
			filterProtocols: []string{"transport-graphsync-filecoinv1"},
			expected:        false,
		},
		{
			name:            "Multiple protocols, one match",
			peerProtocols:   []string{"transport-bitswap", "transport-ipfs-gateway-http"},
			filterProtocols: []string{"transport-graphsync-filecoinv1", "transport-ipfs-gateway-http"},
			expected:        true,
		},
		{
			name:            "Unknown protocol for empty peer protocols",
			peerProtocols:   []string{},
			filterProtocols: []string{"unknown"},
			expected:        true,
		},
		{
			name:            "Unknown protocol for non-empty peer protocols",
			peerProtocols:   []string{"transport-bitswap"},
			filterProtocols: []string{"unknown"},
			expected:        false,
		},
		{
			name:            "Unknown or specific protocol for matching non-empty peer protocols",
			peerProtocols:   []string{"transport-bitswap"},
			filterProtocols: []string{"unknown", "transport-bitswap", "transport-ipfs-gateway-http"},
			expected:        true,
		},
		{
			name:            "Unknown or specific protocol for matching empty peer protocols",
			peerProtocols:   []string{},
			filterProtocols: []string{"unknown", "transport-bitswap", "transport-ipfs-gateway-http"},
			expected:        true,
		},
		{
			name:            "Unknown or specific protocol for not matching non-empty peer protocols",
			peerProtocols:   []string{"transport-graphsync-filecoinv1"},
			filterProtocols: []string{"unknown", "transport-bitswap", "transport-ipfs-gateway-http"},
			expected:        false,
		},
		{
			name:            "Case insensitive match",
			peerProtocols:   []string{"TRANSPORT-BITSWAP", "Transport-IPFS-Gateway-HTTP"},
			filterProtocols: []string{"transport-bitswap"},
			expected:        true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := protocolsAllowed(tc.peerProtocols, tc.filterProtocols)
			assert.Equal(t, tc.expected, result, "Unexpected result for test case: %s", tc.name)
		})
	}
}

func TestApplyFilters(t *testing.T) {
	pid, err := peer.Decode("12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn")
	require.NoError(t, err)

	tests := []struct {
		name            string
		provider        *types.PeerRecord
		filterAddrs     []string
		filterProtocols []string
		expected        *types.PeerRecord
	}{
		{
			name: "No filters",
			provider: &types.PeerRecord{
				ID: &pid,
				Addrs: []types.Multiaddr{
					mustMultiaddr(t, "/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit"),
					mustMultiaddr(t, "/ip4/8.8.8.8/udp/4001/quic-v1/webtransport"),
				},
				Protocols: []string{"transport-ipfs-gateway-http"},
			},
			filterAddrs:     []string{},
			filterProtocols: []string{},
			expected: &types.PeerRecord{
				ID: &pid,
				Addrs: []types.Multiaddr{
					mustMultiaddr(t, "/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit"),
					mustMultiaddr(t, "/ip4/8.8.8.8/udp/4001/quic-v1/webtransport"),
				},
				Protocols: []string{"transport-ipfs-gateway-http"},
			},
		},
		{
			name: "Protocol filter",
			provider: &types.PeerRecord{
				ID: &pid,
				Addrs: []types.Multiaddr{
					mustMultiaddr(t, "/ip4/127.0.0.1/tcp/4001"),
					mustMultiaddr(t, "/ip4/127.0.0.1/udp/4001/quic-v1"),
					mustMultiaddr(t, "/ip4/127.0.0.1/tcp/4001/ws"),
					mustMultiaddr(t, "/ip4/102.101.1.1/tcp/4001/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit"),
					mustMultiaddr(t, "/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit"),
					mustMultiaddr(t, "/ip4/8.8.8.8/udp/4001/quic-v1/webtransport"),
				},
				Protocols: []string{"transport-ipfs-gateway-http"},
			},
			filterAddrs:     []string{},
			filterProtocols: []string{"transport-ipfs-gateway-http", "transport-bitswap"},
			expected: &types.PeerRecord{
				ID: &pid,
				Addrs: []types.Multiaddr{
					mustMultiaddr(t, "/ip4/127.0.0.1/tcp/4001"),
					mustMultiaddr(t, "/ip4/127.0.0.1/udp/4001/quic-v1"),
					mustMultiaddr(t, "/ip4/127.0.0.1/tcp/4001/ws"),
					mustMultiaddr(t, "/ip4/102.101.1.1/tcp/4001/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit"),
					mustMultiaddr(t, "/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit"),
					mustMultiaddr(t, "/ip4/8.8.8.8/udp/4001/quic-v1/webtransport"),
				},
				Protocols: []string{"transport-ipfs-gateway-http"},
			},
		},
		{
			name: "Address filter",
			provider: &types.PeerRecord{
				ID: &pid,
				Addrs: []types.Multiaddr{
					mustMultiaddr(t, "/ip4/127.0.0.1/tcp/4001"),
					mustMultiaddr(t, "/ip4/127.0.0.1/udp/4001/quic-v1"),
					mustMultiaddr(t, "/ip4/127.0.0.1/tcp/4001/ws"),
					mustMultiaddr(t, "/ip4/127.0.0.1/udp/4001/webrtc-direct/certhash/uEiCZqN653gMqxrWNmYuNg7Emwb-wvtsuzGE3XD6rypViZA"),
					mustMultiaddr(t, "/ip4/102.101.1.1/tcp/4001/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit"),
					mustMultiaddr(t, "/ip4/102.101.1.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWEjsGPUQJ4Ej3d1Jcg4VckWhFbhc6mkGunMm1faeSzZMu/p2p-circuit"),
					mustMultiaddr(t, "/ip4/8.8.8.8/udp/4001/quic-v1/webtransport"),
				},
				Protocols: []string{"transport-ipfs-gateway-http"},
			},
			filterAddrs:     []string{"webtransport", "wss", "webrtc-direct", "!p2p-circuit"},
			filterProtocols: []string{"transport-ipfs-gateway-http", "transport-bitswap"},
			expected: &types.PeerRecord{
				ID: &pid,
				Addrs: []types.Multiaddr{
					mustMultiaddr(t, "/ip4/127.0.0.1/udp/4001/webrtc-direct/certhash/uEiCZqN653gMqxrWNmYuNg7Emwb-wvtsuzGE3XD6rypViZA"),
					mustMultiaddr(t, "/ip4/8.8.8.8/udp/4001/quic-v1/webtransport"),
				},
				Protocols: []string{"transport-ipfs-gateway-http"},
			},
		},
		{
			name: "Unknown protocol filter",
			provider: &types.PeerRecord{
				ID: &pid,
				Addrs: []types.Multiaddr{
					mustMultiaddr(t, "/ip4/8.8.8.8/udp/4001/quic-v1/webtransport"),
				},
			},
			filterAddrs:     []string{},
			filterProtocols: []string{"unknown"},
			expected: &types.PeerRecord{
				ID: &pid,
				Addrs: []types.Multiaddr{
					mustMultiaddr(t, "/ip4/8.8.8.8/udp/4001/quic-v1/webtransport"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyFilters(tt.provider, tt.filterAddrs, tt.filterProtocols)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestApplyAddrFilterWithURLs verifies that applyAddrFilter handles the
// duck-typed Addresses used by GenericRecord, where the same list can contain
// multiaddrs, HTTP(S) URLs, and non-standard URI schemes.
// This is the core IPIP-518 filtering scenario that differs from PeerRecord
// (which only has multiaddrs).
func TestApplyAddrFilterWithURLs(t *testing.T) {
	maAddr := types.NewAddressFromMultiaddr(mustRawMultiaddr(t, "/ip4/127.0.0.1/tcp/4001"))
	httpsURL := mustAddr(t, "https://gateway.example.com")
	httpURL := mustAddr(t, "http://gateway.example.com:8080")
	fooURL := mustAddr(t, "foo://custom.example.com")
	tcpURL := mustAddr(t, "tcp://192.168.1.1:4001")
	tlsHTTPma := types.NewAddressFromMultiaddr(mustRawMultiaddr(t, "/dns4/example.com/tcp/443/tls/http"))
	httpsMA := types.NewAddressFromMultiaddr(mustRawMultiaddr(t, "/dns/example.com/tcp/443/https"))

	allAddrs := types.Addresses{maAddr, httpsURL, httpURL, fooURL, tcpURL, tlsHTTPma, httpsMA}

	tests := []struct {
		name     string
		filter   []string
		expected types.Addresses
	}{
		{
			name:     "no filter returns all",
			filter:   []string{},
			expected: allAddrs,
		},
		{
			// "http" filter matches:
			// - http:// and https:// URLs (HTTPS is HTTP over TLS)
			// - multiaddrs with /http (including /tls/http)
			// This is the expected behavior for clients looking for
			// HTTP-capable providers regardless of TLS.
			name:   "http matches http and https URLs and /http multiaddrs",
			filter: []string{"http"},
			expected: types.Addresses{
				httpsURL,  // https:// URL matches "http"
				httpURL,   // http:// URL matches "http"
				tlsHTTPma, // /tls/http multiaddr matches "http"
			},
		},
		{
			// "https" filter is stricter than "http":
			// - matches https:// URLs
			// - matches /https and /tls/http multiaddrs
			// - does NOT match plain http:// URLs
			name:   "https matches https URLs and /tls/http and /https multiaddrs",
			filter: []string{"https"},
			expected: types.Addresses{
				httpsURL,  // https:// URL
				tlsHTTPma, // /tls/http multiaddr
				httpsMA,   // /https multiaddr
			},
		},
		{
			// Non-standard URI schemes are matched by exact scheme name.
			name:     "foo matches foo:// URLs only",
			filter:   []string{"foo"},
			expected: types.Addresses{fooURL},
		},
		{
			// tcp:// as a URI scheme matches the "tcp" filter via scheme matching.
			// Multiaddrs with tcp in the protocol stack also match.
			name:   "tcp matches tcp:// URLs and /tcp multiaddrs",
			filter: []string{"tcp"},
			expected: types.Addresses{
				maAddr,    // /ip4/.../tcp/4001
				tcpURL,    // tcp://...
				tlsHTTPma, // /dns4/.../tcp/443/tls/http
				httpsMA,   // /dns/.../tcp/443/https
			},
		},
		{
			// Negative filters exclude addresses that match the protocol.
			name:   "exclude http removes http and https URLs and /http multiaddrs",
			filter: []string{"!http"},
			expected: types.Addresses{
				maAddr, // /ip4/.../tcp/4001 (no /http protocol)
				fooURL,
				tcpURL,
				httpsMA, // /https is not /http
			},
		},
		{
			// Combining positive and negative filters.
			name:   "tcp without http",
			filter: []string{"tcp", "!http"},
			expected: types.Addresses{
				maAddr,  // /ip4/.../tcp/4001
				tcpURL,  // tcp://... (has tcp scheme, no http)
				httpsMA, // /dns/.../tcp/443/https (has tcp, /https is not /http)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyAddrFilter(allAddrs, tt.filter)

			require.Len(t, result, len(tt.expected), "wrong number of addresses")
			for i, want := range tt.expected {
				assert.Equal(t, want.String(), result[i].String(), "address %d mismatch", i)
			}
		})
	}
}

// TestApplyGenericFilters verifies filtering of GenericRecord, which is the
// primary use case introduced by IPIP-518. GenericRecord differs from
// PeerRecord in that its Addrs field accepts both multiaddrs and URIs.
func TestApplyGenericFilters(t *testing.T) {
	t.Run("no filters returns record unchanged", func(t *testing.T) {
		rec := &types.GenericRecord{
			Schema:    types.SchemaGeneric,
			ID:        "peer1",
			Addrs:     types.Addresses{mustAddr(t, "https://example.com")},
			Protocols: []string{"transport-ipfs-gateway-http"},
		}

		result := applyGenericFilters(rec, nil, nil)
		require.NotNil(t, result)
		assert.Len(t, result.Addrs, 1)
	})

	t.Run("protocol filter removes non-matching record", func(t *testing.T) {
		// Record advertises gateway-http but filter asks for bitswap.
		// The entire record should be removed (nil return).
		rec := &types.GenericRecord{
			Schema:    types.SchemaGeneric,
			ID:        "peer1",
			Addrs:     types.Addresses{mustAddr(t, "https://example.com")},
			Protocols: []string{"transport-ipfs-gateway-http"},
		}

		result := applyGenericFilters(rec, nil, []string{"transport-bitswap"})
		assert.Nil(t, result, "record should be filtered out when protocol does not match")
	})

	t.Run("protocol filter keeps matching record", func(t *testing.T) {
		rec := &types.GenericRecord{
			Schema:    types.SchemaGeneric,
			ID:        "peer1",
			Addrs:     types.Addresses{mustAddr(t, "https://example.com")},
			Protocols: []string{"transport-ipfs-gateway-http"},
		}

		result := applyGenericFilters(rec, nil, []string{"transport-ipfs-gateway-http"})
		require.NotNil(t, result)
		assert.Equal(t, "peer1", result.ID)
	})

	t.Run("addr filter on mixed multiaddr and URL addresses", func(t *testing.T) {
		// A typical GenericRecord from a provider that supports both
		// bitswap (multiaddr) and HTTP gateway (URL).
		// Filtering for "http" should keep the URL and remove the multiaddr.
		rec := &types.GenericRecord{
			Schema: types.SchemaGeneric,
			ID:     "peer1",
			Addrs: types.Addresses{
				types.NewAddressFromMultiaddr(mustRawMultiaddr(t, "/ip4/127.0.0.1/udp/4001/quic-v1")),
				mustAddr(t, "https://gateway.example.com"),
				mustAddr(t, "foo://custom.example.com"),
			},
			Protocols: []string{"transport-bitswap", "transport-ipfs-gateway-http"},
		}

		result := applyGenericFilters(rec, []string{"http"}, nil)
		require.NotNil(t, result)
		require.Len(t, result.Addrs, 1)
		assert.Equal(t, "https://gateway.example.com", result.Addrs[0].String())
	})

	t.Run("addr filter removes record when no addresses match", func(t *testing.T) {
		// When filtering removes all addresses, the record itself should be
		// removed (nil return) since it has no usable connectivity info.
		rec := &types.GenericRecord{
			Schema: types.SchemaGeneric,
			ID:     "peer1",
			Addrs: types.Addresses{
				mustAddr(t, "foo://custom.example.com"),
			},
			Protocols: []string{"transport-foo"},
		}

		result := applyGenericFilters(rec, []string{"http"}, nil)
		assert.Nil(t, result, "record should be removed when no addresses match the filter")
	})

	t.Run("unknown addr filter keeps record with no addresses", func(t *testing.T) {
		// "unknown" in the addr filter means "include providers whose
		// addresses are unknown or cannot be parsed".
		// A record with no addresses should be kept.
		rec := &types.GenericRecord{
			Schema:    types.SchemaGeneric,
			ID:        "peer1",
			Protocols: []string{"transport-ipfs-gateway-http"},
		}

		result := applyGenericFilters(rec, []string{"unknown"}, nil)
		require.NotNil(t, result, "record with no addrs should be kept when filter contains 'unknown'")
	})
}

// TestApplyFiltersToIterMixedSchemas verifies that the iterator-level filter
// correctly processes a stream containing both PeerRecord and GenericRecord.
// This is the real-world scenario: a routing server returns a mix of schema
// types and the client needs to filter all of them consistently.
func TestApplyFiltersToIterMixedSchemas(t *testing.T) {
	pid, err := peer.Decode("12D3KooWM8sovaEGU1bmiWGWAzvs47DEcXKZZTuJnpQyVTkRs2Vn")
	require.NoError(t, err)

	records := []iter.Result[types.Record]{
		// PeerRecord with bitswap transport (multiaddr only)
		{Val: &types.PeerRecord{
			Schema:    types.SchemaPeer,
			ID:        &pid,
			Protocols: []string{"transport-bitswap"},
			Addrs: []types.Multiaddr{
				mustMultiaddr(t, "/ip4/127.0.0.1/tcp/4001"),
				mustMultiaddr(t, "/ip4/127.0.0.1/udp/4001/quic-v1"),
			},
		}},
		// GenericRecord with gateway transport (URL + multiaddr)
		{Val: &types.GenericRecord{
			Schema:    types.SchemaGeneric,
			ID:        "gateway-provider",
			Protocols: []string{"transport-ipfs-gateway-http"},
			Addrs: types.Addresses{
				mustAddr(t, "https://gateway.example.com"),
				types.NewAddressFromMultiaddr(mustRawMultiaddr(t, "/dns4/gateway.example.com/tcp/443/tls/http")),
				mustAddr(t, "foo://unrelated.example.com"),
			},
		}},
		// GenericRecord that should be filtered out by protocol
		{Val: &types.GenericRecord{
			Schema:    types.SchemaGeneric,
			ID:        "other-provider",
			Protocols: []string{"transport-graphsync-filecoinv1"},
			Addrs:     types.Addresses{mustAddr(t, "https://filecoin.example.com")},
		}},
	}

	t.Run("filter by protocol keeps matching schemas", func(t *testing.T) {
		input := iter.FromSlice(records)
		filtered := ApplyFiltersToIter(input, nil, []string{"transport-ipfs-gateway-http"})
		defer filtered.Close()

		var results []types.Record
		for filtered.Next() {
			v := filtered.Val()
			require.NoError(t, v.Err)
			results = append(results, v.Val)
		}

		// Only the GenericRecord with gateway-http protocol should survive
		require.Len(t, results, 1)
		gr, ok := results[0].(*types.GenericRecord)
		require.True(t, ok, "expected GenericRecord")
		assert.Equal(t, "gateway-provider", gr.ID)
	})

	t.Run("filter by addr with http keeps URLs and /http multiaddrs", func(t *testing.T) {
		input := iter.FromSlice(records)
		filtered := ApplyFiltersToIter(input, []string{"http"}, nil)
		defer filtered.Close()

		var results []types.Record
		for filtered.Next() {
			v := filtered.Val()
			require.NoError(t, v.Err)
			results = append(results, v.Val)
		}

		// PeerRecord has no /http addrs, so it's filtered out.
		// GenericRecord "gateway-provider" has https URL and /tls/http multiaddr.
		// GenericRecord "other-provider" has https URL.
		require.Len(t, results, 2)

		gr1, ok := results[0].(*types.GenericRecord)
		require.True(t, ok)
		assert.Equal(t, "gateway-provider", gr1.ID)
		// foo:// address should be filtered out, only http-capable ones remain
		assert.Len(t, gr1.Addrs, 2)

		gr2, ok := results[1].(*types.GenericRecord)
		require.True(t, ok)
		assert.Equal(t, "other-provider", gr2.ID)
	})

	t.Run("combined protocol and addr filter", func(t *testing.T) {
		input := iter.FromSlice(records)
		filtered := ApplyFiltersToIter(input, []string{"http"}, []string{"transport-ipfs-gateway-http"})
		defer filtered.Close()

		var results []types.Record
		for filtered.Next() {
			v := filtered.Val()
			require.NoError(t, v.Err)
			results = append(results, v.Val)
		}

		// Only gateway-provider matches both protocol and addr filters
		require.Len(t, results, 1)
		gr, ok := results[0].(*types.GenericRecord)
		require.True(t, ok)
		assert.Equal(t, "gateway-provider", gr.ID)
		assert.Len(t, gr.Addrs, 2, "https URL and /tls/http multiaddr should survive")
	})
}

func mustMultiaddr(t *testing.T, s string) types.Multiaddr {
	t.Helper()
	ma, err := multiaddr.NewMultiaddr(s)
	require.NoError(t, err)
	return types.Multiaddr{Multiaddr: ma}
}

// mustRawMultiaddr returns a raw multiaddr.Multiaddr (not types.Multiaddr).
func mustRawMultiaddr(t *testing.T, s string) multiaddr.Multiaddr {
	t.Helper()
	ma, err := multiaddr.NewMultiaddr(s)
	require.NoError(t, err)
	return ma
}

// mustAddr creates a types.Address from a string or fails the test.
func mustAddr(t *testing.T, s string) types.Address {
	t.Helper()
	a, err := types.NewAddress(s)
	require.NoError(t, err)
	return a
}
