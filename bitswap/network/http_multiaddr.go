package network

import (
	"fmt"
	"net/url"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// ExtractHTTPAddress extracts the HTTP schema+host+port from a multiaddress and returns a *url.URL.
func ExtractHTTPAddress(ma multiaddr.Multiaddr) (*url.URL, error) {
	components := ma.Protocols()
	var host, port, schema string

	for _, comp := range components {
		switch comp.Name {
		case "dns", "dns4", "dns6", "ip4", "ip6":
			hostVal, err := ma.ValueForProtocol(comp.Code)
			if err != nil {
				return nil, fmt.Errorf("failed to extract host: %w", err)
			}
			host = hostVal
		case "tcp", "udp":
			portVal, err := ma.ValueForProtocol(comp.Code)
			if err != nil {
				return nil, fmt.Errorf("failed to extract port: %w", err)
			}
			port = portVal
		case "http", "https":
			schema = comp.Name
		}
	}

	if host == "" || port == "" || schema == "" {
		return nil, fmt.Errorf("multiaddress is missing required components (host/port)")
	}

	// Construct the URL object
	address := fmt.Sprintf("%s://%s:%s", schema, host, port)
	parsedURL, err := url.Parse(address)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	return parsedURL, nil
}

// ExtractURLsFromPeer extracts all HTTP schema+host+port addresses as *url.URL from a peer.AddrInfo object.
func ExtractURLsFromPeer(info peer.AddrInfo) []*url.URL {
	var addresses []*url.URL

	for _, addr := range info.Addrs {
		httpAddress, err := ExtractHTTPAddress(addr)
		if err != nil {
			// Skip invalid or non-HTTP addresses but continue with others
			continue
		}
		addresses = append(addresses, httpAddress)
	}

	return addresses
}

// SplitHTTPAddrs splits a peer.AddrInfo into two: one containing HTTP/HTTPS addresses, and the other containing the rest.
func SplitHTTPAddrs(pi peer.AddrInfo) (httpPeer peer.AddrInfo, otherPeer peer.AddrInfo) {
	httpPeer.ID = pi.ID
	otherPeer.ID = pi.ID

	for _, addr := range pi.Addrs {
		if isHTTPAddress(addr) {
			httpPeer.Addrs = append(httpPeer.Addrs, addr)
		} else {
			otherPeer.Addrs = append(otherPeer.Addrs, addr)
		}
	}

	return
}

// isHTTPAddress checks if a multiaddress is an HTTP or HTTPS address.
func isHTTPAddress(ma multiaddr.Multiaddr) bool {
	protocols := ma.Protocols()
	for _, proto := range protocols {
		if proto.Name == "http" || proto.Name == "https" {
			return true
		}
	}
	return false
}
