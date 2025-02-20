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
		sni       string
		expectErr bool
	}{
		{
			name:  "Valid HTTP multiaddress with DNS",
			maStr: "/dns4/example.com/tcp/8080/http",
			want: &url.URL{
				Scheme: "http",
				Host:   "example.com:8080",
			},
			expectErr: true, // error due to non-local address and no TLS.
		},
		{
			name:  "Valid HTTPS multiaddress with DNS4",
			maStr: "/dns4/example.com/tcp/443/https",
			want: &url.URL{
				Scheme: "https",
				Host:   "example.com:443",
			},
			expectErr: false,
		},
		{
			name:  "Valid HTTPS multiaddress with DNS6",
			maStr: "/dns6/example.com/tcp/443/https",
			want: &url.URL{
				Scheme: "https",
				Host:   "example.com:443",
			},
			expectErr: false,
		},
		{
			name:  "Valid HTTPS multiaddress with DNS",
			maStr: "/dns/example.com/tcp/443/https",
			want: &url.URL{
				Scheme: "https",
				Host:   "example.com:443",
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
			expectErr: true, // error due to wss: we need HTTPs
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
		{
			name:  "tls/http multiaddress without sni",
			maStr: "/ip4/127.0.0.1/tcp/8080/tls/http",
			want: &url.URL{
				Scheme: "https",
				Host:   "127.0.0.1:8080",
			},
			expectErr: false,
		},
		{
			name:  "tls/http with sni",
			maStr: "/dns4/example.com/tcp/443/tls/sni/example2.com/http",
			want: &url.URL{
				Scheme: "https",
				Host:   "example.com:443",
			},
			sni:       "example2.com",
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
				t.Errorf("got: %s", got.URL)
				t.Errorf("ExtractHTTPAddress() error = %v, wantErr %v", err, tt.expectErr)
				return
			}

			if tt.want != nil && (got.URL == nil || got.URL.String() != tt.want.String() || tt.sni != got.SNI) {
				t.Errorf("ExtractHTTPAddress() = %v (%s), want %v (%s)", got.URL, got.SNI, tt.want, tt.sni)
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
				if got[i].URL.String() != tt.want[i].String() {
					t.Errorf("ExtractHTTPAddressesFromPeer() URL at index %d = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}
