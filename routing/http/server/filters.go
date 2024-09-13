package server

import (
	"reflect"
	"slices"
	"strings"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/multiformats/go-multiaddr"
)

// filters implements IPIP-0484

func parseFilter(param string) []string {
	if param == "" {
		return nil
	}
	return strings.Split(strings.ToLower(param), ",")
}

func filterProviders(providers []types.Record, filterAddrs, filterProtocols []string) []types.Record {
	if len(filterAddrs) == 0 && len(filterProtocols) == 0 {
		return providers
	}

	filtered := make([]types.Record, 0, len(providers))

	for _, provider := range providers {
		if schema := provider.GetSchema(); schema == types.SchemaPeer {
			peer, ok := provider.(*types.PeerRecord)
			if !ok {
				logger.Errorw("problem casting find providers result", "Schema", provider.GetSchema(), "Type", reflect.TypeOf(provider).String())
				// if the type assertion fails, we exlude record from results
				continue
			}

			record := applyFilters(peer, filterAddrs, filterProtocols)

			if record != nil {
				filtered = append(filtered, record)
			}

		} else {
			// Will we ever encounter the SchemaBitswap type? Evidence seems to suggest that no longer
			logger.Errorw("encountered unknown provider schema", "Schema", provider.GetSchema(), "Type", reflect.TypeOf(provider).String())
		}
	}
	return filtered
}

// Applies the filters. Returns nil if the provider does not pass the protocols filter
// The address filter is more complicated because it potentially modifies the Addrs slice.
func applyFilters(provider *types.PeerRecord, filterAddrs, filterProtocols []string) *types.PeerRecord {
	if !applyProtocolFilter(provider.Protocols, filterProtocols) {
		// If the provider doesn't match any of the passed protocols, the provider is omitted from the response.
		return nil
	}

	// return untouched if there's no filter or filterAddrsQuery contains "unknown" and provider has no addrs
	if len(filterAddrs) == 0 || (len(provider.Addrs) == 0 && slices.Contains(filterAddrs, "unknown")) {
		return provider
	}

	filteredAddrs := applyAddrFilter(provider.Addrs, filterAddrs)

	// If filtering resulted in no addrs, omit the provider
	if len(filteredAddrs) == 0 {
		return nil
	}

	provider.Addrs = filteredAddrs
	return provider
}

// If there are only negative filters, no addresses will be included in the result. The function will return an empty list.
// For an address to be included, it must pass all negative filters AND match at least one positive filter.
func applyAddrFilter(addrs []types.Multiaddr, filterAddrsQuery []string) []types.Multiaddr {
	if len(filterAddrsQuery) == 0 {
		return addrs
	}

	filteredAddrs := make([]types.Multiaddr, 0, len(addrs))

	for _, addr := range addrs {
		protocols := addr.Protocols()
		includeAddr := true

		// First, check all negative filters
		for _, filter := range filterAddrsQuery {
			if strings.HasPrefix(filter, "!") {
				protocolStringFromFilter := strings.TrimPrefix(filter, "!")
				protocolFromFilter := multiaddr.ProtocolWithName(protocolStringFromFilter)
				if containsProtocol(protocols, protocolFromFilter) {
					includeAddr = false
					break
				}
			}
		}

		// If the address passed all negative filters, check positive filters
		if includeAddr {
			for _, filter := range filterAddrsQuery {
				if !strings.HasPrefix(filter, "!") {
					protocolFromFilter := multiaddr.ProtocolWithName(filter)
					if containsProtocol(protocols, protocolFromFilter) {
						filteredAddrs = append(filteredAddrs, addr)
						break
					}
				}
			}
		}
	}
	return filteredAddrs
}

func containsProtocol(protos []multiaddr.Protocol, proto multiaddr.Protocol) bool {
	for _, p := range protos {
		if p.Code == proto.Code {
			return true
		}
	}
	return false
}

func applyProtocolFilter(peerProtocols []string, filterProtocols []string) bool {
	if len(filterProtocols) == 0 {
		// If no filter is passed, do not filter
		return true
	}

	for _, filterProtocol := range filterProtocols {
		if filterProtocol == "unknown" && len(peerProtocols) == 0 {
			return true
		}

		for _, peerProtocol := range peerProtocols {
			if peerProtocol == filterProtocol {
				return true
			}
		}
	}
	return false
}
