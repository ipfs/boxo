package network

import (
	"net/url"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

func TestExtractHTTPAddress(t *testing.T) {
	tests := []struct {
		name      string
		maStr     string
		want      *url.URL
		expectErr bool
	}{
		{
			name:  "Valid HTTP multiaddress with DNS",
			maStr: "/dns4/example.com/tcp/8080/http",
			want: &url.URL{
				Scheme: "http",
				Host:   "example.com:8080",
			},
			expectErr: false,
		},
		{
			name:  "Valid HTTPS multiaddress with DNS",
			maStr: "/dns4/example.com/tcp/443/https",
			want: &url.URL{
				Scheme: "https",
				Host:   "example.com:443",
			},
			expectErr: false,
		},
		{
			name:      "Valid WSS multiaddress with DNS",
			maStr:     "/dns4/example.com/tcp/443/wss",
			want:      nil,
			expectErr: true,
		},
		{
			name:  "Valid HTTP multiaddress with IP4",
			maStr: "/ip4/127.0.0.1/tcp/8080/http",
			want: &url.URL{
				Scheme: "http",
				Host:   "127.0.0.1:8080",
			},
			expectErr: false,
		},
		{
			name:      "Missing port",
			maStr:     "/dns4/example.com/http",
			want:      nil,
			expectErr: true,
		},
		{
			name:      "Invalid multiaddress",
			maStr:     "/dns4/example.com/tcp/abc/http",
			want:      nil,
			expectErr: true,
		},
		{
			name:      "Unsupported protocol",
			maStr:     "/unix/tmp/socket",
			want:      nil,
			expectErr: true,
		},
		{
			name:  "Valid HTTP multiaddress with IP6",
			maStr: "/ip6/::1/tcp/8080/http",
			want: &url.URL{
				Scheme: "http",
				Host:   "::1:8080",
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ma, err := multiaddr.NewMultiaddr(tt.maStr)
			if err != nil {
				if !tt.expectErr {
					t.Fatalf("failed to create multiaddress: %v", err)
				}
				return
			}

			got, err := ExtractHTTPAddress(ma)
			if (err != nil) != tt.expectErr {
				t.Errorf("got: %s", got)
				t.Errorf("ExtractHTTPAddress() error = %v, wantErr %v", err, tt.expectErr)
				return
			}

			if tt.want != nil && (got == nil || got.String() != tt.want.String()) {
				t.Errorf("ExtractHTTPAddress() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractHTTPAddressesFromPeer(t *testing.T) {
	tests := []struct {
		name     string
		peerInfo *peer.AddrInfo
		want     []*url.URL
	}{
		{
			name: "Valid peer with multiple addresses",
			peerInfo: &peer.AddrInfo{
				ID: "12D3KooWQrKv5jtT5anTrKjwgb5dkt7DYHhTT9JzLs7dABZ1mkTf",
				Addrs: []multiaddr.Multiaddr{
					multiaddr.StringCast("/dns4/example.com/tcp/8080/http"),
					multiaddr.StringCast("/ip4/127.0.0.1/tcp/8081/http"),
					multiaddr.StringCast("/ip4/127.0.0.1/tcp/9000"), // Non-HTTP
				},
			},
			want: []*url.URL{
				{
					Scheme: "http",
					Host:   "example.com:8080",
				},
				{
					Scheme: "http",
					Host:   "127.0.0.1:8081",
				},
			},
		},
		{
			name: "No valid HTTP addresses in peer",
			peerInfo: &peer.AddrInfo{
				ID: "12D3KooWQrKv5jtT5anTrKjwgb5dkt7DYHhTT9JzLs7dABZ1mkTf",
				Addrs: []multiaddr.Multiaddr{
					multiaddr.StringCast("/ip4/127.0.0.1/tcp/9000"), // Non-HTTP
				},
			},
			want: nil,
		},
		{
			name: "Empty peer info",
			peerInfo: &peer.AddrInfo{
				ID:    "12D3KooWQrKv5jtT5anTrKjwgb5dkt7DYHhTT9JzLs7dABZ1mkTf",
				Addrs: []multiaddr.Multiaddr{},
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractURLsFromPeer(*tt.peerInfo)
			if len(got) != len(tt.want) {
				t.Errorf("ExtractHTTPAddressesFromPeer() = %v, want %v", got, tt.want)
				return
			}

			// Compare URLs
			for i := range got {
				if got[i].String() != tt.want[i].String() {
					t.Errorf("ExtractHTTPAddressesFromPeer() URL at index %d = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}
