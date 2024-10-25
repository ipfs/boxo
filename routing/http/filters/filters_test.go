package filters

import (
	"testing"

	"github.com/ipfs/boxo/routing/http/types"
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

	addrs := []types.Multiaddr{
		{Multiaddr: addr1},
		{Multiaddr: addr2},
		{Multiaddr: addr3},
		{Multiaddr: addr4},
		{Multiaddr: addr5},
		{Multiaddr: addr6},
		{Multiaddr: addr7},
		{Multiaddr: addr8},
	}

	testCases := []struct {
		name          string
		filterAddrs   []string
		expectedAddrs []types.Multiaddr
	}{
		{
			name:          "No filter",
			filterAddrs:   []string{},
			expectedAddrs: addrs,
		},
		{
			name:          "Filter TCP",
			filterAddrs:   []string{"tcp"},
			expectedAddrs: []types.Multiaddr{{Multiaddr: addr1}, {Multiaddr: addr3}, {Multiaddr: addr4}, {Multiaddr: addr7}},
		},
		{
			name:          "Filter UDP",
			filterAddrs:   []string{"udp"},
			expectedAddrs: []types.Multiaddr{{Multiaddr: addr2}, {Multiaddr: addr5}, {Multiaddr: addr6}, {Multiaddr: addr8}},
		},
		{
			name:          "Filter WebSocket",
			filterAddrs:   []string{"ws"},
			expectedAddrs: []types.Multiaddr{{Multiaddr: addr3}},
		},
		{
			name:          "Exclude TCP",
			filterAddrs:   []string{"!tcp"},
			expectedAddrs: []types.Multiaddr{{Multiaddr: addr2}, {Multiaddr: addr5}, {Multiaddr: addr6}, {Multiaddr: addr8}},
		},
		{
			name:          "Filter TCP addresses that don't have WebSocket and p2p-circuit",
			filterAddrs:   []string{"tcp", "!ws", "!wss", "!p2p-circuit"},
			expectedAddrs: []types.Multiaddr{{Multiaddr: addr1}},
		},
		{
			name:          "Include WebTransport and exclude p2p-circuit",
			filterAddrs:   []string{"webtransport", "!p2p-circuit"},
			expectedAddrs: []types.Multiaddr{{Multiaddr: addr8}},
		},
		{
			name:          "empty for unknown protocol nae",
			filterAddrs:   []string{"fakeproto"},
			expectedAddrs: []types.Multiaddr{},
		},
		{
			name:          "Include WebTransport but ignore unknown protocol name",
			filterAddrs:   []string{"webtransport", "fakeproto"},
			expectedAddrs: []types.Multiaddr{{Multiaddr: addr6}, {Multiaddr: addr8}},
		},
		{
			name:          "Multiple filters",
			filterAddrs:   []string{"tcp", "ws"},
			expectedAddrs: []types.Multiaddr{{Multiaddr: addr1}, {Multiaddr: addr3}, {Multiaddr: addr4}, {Multiaddr: addr7}},
		},
		{
			name:          "Multiple negative filters",
			filterAddrs:   []string{"!tcp", "!ws"},
			expectedAddrs: []types.Multiaddr{{Multiaddr: addr2}, {Multiaddr: addr5}, {Multiaddr: addr6}, {Multiaddr: addr8}},
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
					if expectedAddr.Multiaddr.Equal(resultAddr.Multiaddr) {
						found = true
						break
					}
				}
				assert.True(t, found, "Expected address not found in test %s result: %s", tc.name, expectedAddr.Multiaddr)
			}

			// Check that each result address is in the expected list
			for _, resultAddr := range result {
				found := false
				for _, expectedAddr := range tc.expectedAddrs {
					if resultAddr.Multiaddr.Equal(expectedAddr.Multiaddr) {
						found = true
						break
					}
				}
				assert.True(t, found, "Unexpected address found in test %s result: %s", tc.name, resultAddr.Multiaddr)
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

func mustMultiaddr(t *testing.T, s string) types.Multiaddr {
	addr, err := multiaddr.NewMultiaddr(s)
	if err != nil {
		t.Fatalf("Failed to create multiaddr: %v", err)
	}
	return types.Multiaddr{Multiaddr: addr}
}
