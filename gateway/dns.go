package gateway

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/libp2p/go-doh-resolver"
	dns "github.com/miekg/dns"
	madns "github.com/multiformats/go-multiaddr-dns"
	"github.com/rs/dnscache"
)

var defaultResolvers = map[string]string{
	"eth.":    "https://resolver.cloudflare-eth.com/dns-query",
	"crypto.": "https://resolver.cloudflare-eth.com/dns-query",
}

func newResolver(url string, opts ...doh.Option) (madns.BasicResolver, error) {
	if !strings.HasPrefix(url, "https://") {
		return nil, fmt.Errorf("invalid resolver url: %s", url)
	}

	return doh.NewResolver(url, opts...)
}

// NewDNSResolver creates a new DNS resolver based on the default resolvers and
// the provided resolvers.
//
// The argument 'resolvers' is a map of [FQDNs] to URLs for custom DNS resolution.
// URLs starting with "https://" indicate [DoH] endpoints. Support for other resolver
// types may be added in the future.
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

	domains := make(map[string]struct{})           // to track overridden default resolvers
	rslvrs := make(map[string]madns.BasicResolver) // to reuse resolvers for the same URL

	for domain, url := range resolvers {
		if domain != "." && !dns.IsFqdn(domain) {
			return nil, fmt.Errorf("invalid domain %s; must be FQDN", domain)
		}

		domains[domain] = struct{}{}
		if url == "" {
			// allow overriding of implicit defaults with the default resolver
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

	// fill in defaults if not overridden by the user
	for domain, url := range defaultResolvers {
		_, ok := domains[domain]
		if ok {
			continue
		}

		rslv, ok := rslvrs[url]
		if !ok {
			rslv, err = newResolver(url)
			if err != nil {
				return nil, fmt.Errorf("bad resolver for %s: %w", domain, err)
			}
			rslvrs[url] = rslv
		}

		opts = append(opts, madns.WithDomainResolver(domain, rslv))
	}

	return madns.NewResolver(opts...)
}

// CachedDNS implements [http.Transport.DialContext], allowing to cache DNS
// requests for a specified amount of time.
type CachedDNS struct {
	resolver  *dnscache.Resolver
	refresher *time.Ticker
}

func NewCachedDNS(refreshInterval time.Duration) *CachedDNS {
	cache := &CachedDNS{
		resolver:  &dnscache.Resolver{},
		refresher: time.NewTicker(refreshInterval),
	}

	// Configure DNS cache to not remove stale records to protect gateway from
	// catastrophic failures like https://github.com/ipfs/bifrost-gateway/issues/34
	options := dnscache.ResolverRefreshOptions{}
	options.ClearUnused = false
	options.PersistOnFailure = true

	// Every refreshInterval we check for updates, but if there is
	// none, or if domain disappears, we keep the last cached version
	go func(cdns *CachedDNS) {
		defer cdns.refresher.Stop()
		for range cdns.refresher.C {
			cdns.resolver.RefreshWithOptions(options)
		}
	}(cache)

	return cache
}
func (cdns *CachedDNS) DialContext(ctx context.Context, network string, addr string) (conn net.Conn, err error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ips, err := cdns.resolver.LookupHost(ctx, host)
	if err != nil {
		return nil, err
	}
	// Try all IPs returned by DNS
	for _, ip := range ips {
		var dialer net.Dialer
		conn, err = dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
		if err == nil {
			break
		}
	}
	return
}

func (cdns *CachedDNS) Close() error {
	cdns.refresher.Stop()
	return nil
}
