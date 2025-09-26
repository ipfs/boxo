package gateway

import (
	"fmt"
	"strings"

	"github.com/libp2p/go-doh-resolver"
	dns "github.com/miekg/dns"
	madns "github.com/multiformats/go-multiaddr-dns"
)

func newResolver(url string, opts ...doh.Option) (madns.BasicResolver, error) {
	if !strings.HasPrefix(url, "https://") && !strings.HasPrefix(url, "http://") {
		return nil, fmt.Errorf("invalid DoH resolver URL: %s", url)
	}

	return doh.NewResolver(url, opts...)
}

// NewDNSResolver creates a new DNS resolver based on the provided resolvers.
//
// The argument 'resolvers' is a map of [FQDNs] to URLs for custom DNS resolution.
// URLs starting with "https://" indicate [DoH] endpoints. Support for other resolver
// types may be added in the future.
//
// If 'resolvers' is nil or empty, the default system DNS resolver will be used.
//
// To use network-specific DNS resolvers (e.g., for .eth or other non-ICANN TLDs),
// use [boxo/autoconf.ExpandDNSResolvers] to merge autoconf-provided resolvers with
// custom resolvers before calling this function.
//
// Example:
//   - Custom resolver for ENS:          "eth." → "https://eth.link/dns-query"
//   - Override the default OS resolver: "."    → "https://doh.applied-privacy.net/query"
//
// [FQDNs]: https://en.wikipedia.org/wiki/Fully_qualified_domain_name
// [DoH]: https://en.wikipedia.org/wiki/DNS_over_HTTPS
func NewDNSResolver(resolvers map[string]string, dohOpts ...doh.Option) (*madns.Resolver, error) {
	var opts []madns.Option
	var err error

	rslvrs := make(map[string]madns.BasicResolver) // to reuse resolvers for the same URL

	for domain, url := range resolvers {
		if domain != "." && !dns.IsFqdn(domain) {
			return nil, fmt.Errorf("invalid domain %s; must be FQDN", domain)
		}

		if url == "" {
			// allow clearing resolver for a domain
			continue
		}

		rslv, ok := rslvrs[url]
		if !ok {
			rslv, err = newResolver(url, dohOpts...)
			if err != nil {
				return nil, fmt.Errorf("bad resolver for %s: %w", domain, err)
			}
			rslvrs[url] = rslv
		}

		if domain != "." {
			opts = append(opts, madns.WithDomainResolver(domain, rslv))
		} else {
			opts = append(opts, madns.WithDefaultResolver(rslv))
		}
	}

	return madns.NewResolver(opts...)
}
