package gateway

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	dns "github.com/miekg/dns"

	mbase "github.com/multiformats/go-multibase"
)

// Specification is the specification of an IPFS Public Gateway.
type Specification struct {
	// Paths is explicit list of path prefixes that should be handled by
	// this gateway. Example: `["/ipfs", "/ipns"]`
	// Useful if you only want to support immutable `/ipfs`.
	Paths []string

	// UseSubdomains indicates whether or not this gateway uses subdomains
	// for IPFS resources instead of paths. That is: http://CID.ipfs.GATEWAY/...
	//
	// If this flag is set, any /ipns/$id and/or /ipfs/$id paths in Paths
	// will be permanently redirected to http://$id.[ipns|ipfs].$gateway/.
	//
	// We do not support using both paths and subdomains for a single domain
	// for security reasons (Origin isolation).
	UseSubdomains bool

	// NoDNSLink configures this gateway to _not_ resolve DNSLink for the
	// specific FQDN provided in `Host` HTTP header. Useful when you want to
	// explicitly allow or refuse hosting a single hostname. To refuse all
	// DNSLinks in `Host` processing, pass noDNSLink to `WithHostname` instead.
	// This flag overrides the global one.
	NoDNSLink bool

	// InlineDNSLink configures this gateway to always inline DNSLink names
	// (FQDN) into a single DNS label in order to interop with wildcard TLS certs
	// and Origin per CID isolation provided by rules like https://publicsuffix.org
	// This should be set to true if you use HTTPS.
	InlineDNSLink bool
}

// WithHostname is a middleware that can wrap an http.Handler in order to parse the
// Host header and translating it to the content path. This is useful for Subdomain
// and DNSLink gateways.
//
// publicGateways configures the behavior of known public gateways. Each key is a
// fully qualified domain name (FQDN).
//
// noDNSLink configures the gateway to _not_ perform DNS TXT record lookups in
// response to requests with values in `Host` HTTP header. This flag can be overridden
// per FQDN in publicGateways.
func WithHostname(next http.Handler, api API, publicGateways map[string]*Specification, noDNSLink bool) http.HandlerFunc {
	gateways := prepareHostnameGateways(publicGateways)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Unfortunately, many (well, ipfs.io) gateways use
		// DNSLink so if we blindly rewrite with DNSLink, we'll
		// break /ipfs links.
		//
		// We fix this by maintaining a list of known gateways
		// and the paths that they serve "gateway" content on.
		// That way, we can use DNSLink for everything else.

		// Support X-Forwarded-Host if added by a reverse proxy
		// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-Host
		host := r.Host
		if xHost := r.Header.Get("X-Forwarded-Host"); xHost != "" {
			host = xHost
		}

		// HTTP Host & Path check: is this one of our  "known gateways"?
		if gw, ok := gateways.isKnownHostname(host); ok {
			// This is a known gateway but request is not using
			// the subdomain feature.

			// Does this gateway _handle_ this path?
			if hasPrefix(r.URL.Path, gw.Paths...) {
				// It does.

				// Should this gateway use subdomains instead of paths?
				if gw.UseSubdomains {
					// Yes, redirect if applicable
					// Example: dweb.link/ipfs/{cid} → {cid}.ipfs.dweb.link
					useInlinedDNSLink := gw.InlineDNSLink
					newURL, err := toSubdomainURL(host, r.URL.Path, r, useInlinedDNSLink, api)
					if err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					if newURL != "" {
						// Set "Location" header with redirect destination.
						// It is ignored by curl in default mode, but will
						// be respected by user agents that follow
						// redirects by default, namely web browsers
						w.Header().Set("Location", newURL)

						// Note: we continue regular gateway processing:
						// HTTP Status Code http.StatusMovedPermanently
						// will be set later, in statusResponseWriter
					}
				}

				// Not a subdomain resource, continue with path processing
				// Example: 127.0.0.1:8080/ipfs/{CID}, ipfs.io/ipfs/{CID} etc
				next.ServeHTTP(w, r)
				return
			}
			// Not a whitelisted path

			// Try DNSLink, if it was not explicitly disabled for the hostname
			if !gw.NoDNSLink && hasDNSLinkRecord(r.Context(), api, host) {
				// rewrite path and handle as DNSLink
				r.URL.Path = "/ipns/" + stripPort(host) + r.URL.Path
				next.ServeHTTP(w, withHostnameContext(r, host))
				return
			}

			// If not, resource does not exist on the hostname, return 404
			http.NotFound(w, r)
			return
		}

		// HTTP Host check: is this one of our subdomain-based "known gateways"?
		// IPFS details extracted from the host: {rootID}.{ns}.{gwHostname}
		// /ipfs/ example: {cid}.ipfs.localhost:8080, {cid}.ipfs.dweb.link
		// /ipns/ example: {libp2p-key}.ipns.localhost:8080, {inlined-dnslink-fqdn}.ipns.dweb.link
		if gw, gwHostname, ns, rootID, ok := gateways.knownSubdomainDetails(host); ok {
			// Looks like we're using a known gateway in subdomain mode.

			// Assemble original path prefix.
			pathPrefix := "/" + ns + "/" + rootID

			// Retrieve whether or not we should inline DNSLink.
			useInlinedDNSLink := gw.InlineDNSLink

			// Does this gateway _handle_ subdomains AND this path?
			if !(gw.UseSubdomains && hasPrefix(pathPrefix, gw.Paths...)) {
				// If not, resource does not exist, return 404
				http.NotFound(w, r)
				return
			}

			// Check if rootID is a valid CID
			if rootCID, err := cid.Decode(rootID); err == nil {
				// Do we need to redirect root CID to a canonical DNS representation?
				dnsCID, err := toDNSLabel(rootID, rootCID)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				if !strings.HasPrefix(r.Host, dnsCID) {
					dnsPrefix := "/" + ns + "/" + dnsCID
					newURL, err := toSubdomainURL(gwHostname, dnsPrefix+r.URL.Path, r, useInlinedDNSLink, api)
					if err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					if newURL != "" {
						// Redirect to deterministic CID to ensure CID
						// always gets the same Origin on the web
						http.Redirect(w, r, newURL, http.StatusMovedPermanently)
						return
					}
				}

				// Do we need to fix multicodec in PeerID represented as CIDv1?
				if isPeerIDNamespace(ns) {
					if rootCID.Type() != cid.Libp2pKey {
						newURL, err := toSubdomainURL(gwHostname, pathPrefix+r.URL.Path, r, useInlinedDNSLink, api)
						if err != nil {
							http.Error(w, err.Error(), http.StatusBadRequest)
							return
						}
						if newURL != "" {
							// Redirect to CID fixed inside of toSubdomainURL()
							http.Redirect(w, r, newURL, http.StatusMovedPermanently)
							return
						}
					}
				}
			} else { // rootID is not a CID..
				// Check if rootID is a single DNS label with an inlined
				// DNSLink FQDN a single DNS label. We support this so
				// loading DNSLink names over TLS "just works" on public
				// HTTP gateways.
				//
				// Rationale for doing this can be found under "Option C"
				// at: https://github.com/ipfs/in-web-browsers/issues/169
				//
				// TLDR is:
				// https://dweb.link/ipns/my.v-long.example.com
				// can be loaded from a subdomain gateway with a wildcard
				// TLS cert if represented as a single DNS label:
				// https://my-v--long-example-com.ipns.dweb.link
				if ns == "ipns" && !strings.Contains(rootID, ".") {
					// if there is no TXT recordfor rootID
					if !hasDNSLinkRecord(r.Context(), api, rootID) {
						// my-v--long-example-com → my.v-long.example.com
						dnslinkFQDN := toDNSLinkFQDN(rootID)
						if hasDNSLinkRecord(r.Context(), api, dnslinkFQDN) {
							// update path prefix to use real FQDN with DNSLink
							pathPrefix = "/ipns/" + dnslinkFQDN
						}
					}
				}
			}

			// Rewrite the path to not use subdomains
			r.URL.Path = pathPrefix + r.URL.Path

			// Serve path request
			next.ServeHTTP(w, withHostnameContext(r, gwHostname))
			return
		}

		// We don't have a known gateway. Fallback on DNSLink lookup

		// Wildcard HTTP Host check:
		// 1. is wildcard DNSLink enabled (Gateway.NoDNSLink=false)?
		// 2. does Host header include a fully qualified domain name (FQDN)?
		// 3. does DNSLink record exist in DNS?
		if !noDNSLink && hasDNSLinkRecord(r.Context(), api, host) {
			// rewrite path and handle as DNSLink
			r.URL.Path = "/ipns/" + stripPort(host) + r.URL.Path
			ctx := context.WithValue(r.Context(), DNSLinkHostnameKey, host)
			next.ServeHTTP(w, withHostnameContext(r.WithContext(ctx), host))
			return
		}

		// else, treat it as an old school gateway, I guess.
		next.ServeHTTP(w, r)

	})
}

// Extends request context to include hostname of a canonical gateway root
// (subdomain root or dnslink fqdn)
func withHostnameContext(r *http.Request, hostname string) *http.Request {
	// This is required for links on directory listing pages to work correctly
	// on subdomain and dnslink gateways. While DNSlink could read value from
	// Host header, subdomain gateways have more comples rules (knownSubdomainDetails)
	// More: https://github.com/ipfs/dir-index-html/issues/42
	// nolint: staticcheck // non-backward compatible change
	ctx := context.WithValue(r.Context(), GatewayHostnameKey, hostname)
	return r.WithContext(ctx)
}

// isDomainNameAndNotPeerID returns bool if string looks like a valid DNS name AND is not a PeerID
func isDomainNameAndNotPeerID(hostname string) bool {
	if len(hostname) == 0 {
		return false
	}
	if _, err := peer.Decode(hostname); err == nil {
		return false
	}
	_, ok := dns.IsDomainName(hostname)
	return ok
}

// hasDNSLinkRecord returns if a DNS TXT record exists for the provided host.
func hasDNSLinkRecord(ctx context.Context, api API, host string) bool {
	dnslinkName := stripPort(host)

	if !isDomainNameAndNotPeerID(dnslinkName) {
		return false
	}

	_, err := api.GetDNSLinkRecord(ctx, dnslinkName)
	return err == nil
}

func isSubdomainNamespace(ns string) bool {
	switch ns {
	case "ipfs", "ipns", "p2p", "ipld":
		// Note: 'p2p' and 'ipld' is only kept here for compatibility with Kubo.
		return true
	default:
		return false
	}
}

func isPeerIDNamespace(ns string) bool {
	switch ns {
	case "ipns", "p2p":
		// Note: 'p2p' and 'ipld' is only kept here for compatibility with Kubo.
		return true
	default:
		return false
	}
}

// Label's max length in DNS (https://tools.ietf.org/html/rfc1034#page-7)
const dnsLabelMaxLength int = 63

// Converts a CID to DNS-safe representation that fits in 63 characters
func toDNSLabel(rootID string, rootCID cid.Cid) (dnsCID string, err error) {
	// Return as-is if things fit
	if len(rootID) <= dnsLabelMaxLength {
		return rootID, nil
	}

	// Convert to Base36 and see if that helped
	rootID, err = cid.NewCidV1(rootCID.Type(), rootCID.Hash()).StringOfBase(mbase.Base36)
	if err != nil {
		return "", err
	}
	if len(rootID) <= dnsLabelMaxLength {
		return rootID, nil
	}

	// Can't win with DNS at this point, return error
	return "", fmt.Errorf("CID incompatible with DNS label length limit of 63: %s", rootID)
}

// Returns true if HTTP request involves TLS certificate.
// See https://github.com/ipfs/in-web-browsers/issues/169 to understand how it
// impacts DNSLink websites on public gateways.
func isHTTPSRequest(r *http.Request) bool {
	// X-Forwarded-Proto if added by a reverse proxy
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-Proto
	xproto := r.Header.Get("X-Forwarded-Proto")
	// Is request a native TLS (not used atm, but future-proofing)
	// or a proxied HTTPS (eg. go-ipfs behind nginx at a public gw)?
	return r.URL.Scheme == "https" || xproto == "https"
}

// Converts a FQDN to DNS-safe representation that fits in 63 characters:
// my.v-long.example.com → my-v--long-example-com
func toDNSLinkDNSLabel(fqdn string) (dnsLabel string, err error) {
	dnsLabel = strings.ReplaceAll(fqdn, "-", "--")
	dnsLabel = strings.ReplaceAll(dnsLabel, ".", "-")
	if len(dnsLabel) > dnsLabelMaxLength {
		return "", fmt.Errorf("DNSLink representation incompatible with DNS label length limit of 63: %s", dnsLabel)
	}
	return dnsLabel, nil
}

// Converts a DNS-safe representation of DNSLink FQDN to real FQDN:
// my-v--long-example-com → my.v-long.example.com
func toDNSLinkFQDN(dnsLabel string) (fqdn string) {
	fqdn = strings.ReplaceAll(dnsLabel, "--", "@") // @ placeholder is unused in DNS labels
	fqdn = strings.ReplaceAll(fqdn, "-", ".")
	fqdn = strings.ReplaceAll(fqdn, "@", "-")
	return fqdn
}

// Converts a hostname/path to a subdomain-based URL, if applicable.
func toSubdomainURL(hostname, path string, r *http.Request, inlineDNSLink bool, api API) (redirURL string, err error) {
	var scheme, ns, rootID, rest string

	query := r.URL.RawQuery
	parts := strings.SplitN(path, "/", 4)
	isHTTPS := isHTTPSRequest(r)
	safeRedirectURL := func(in string) (out string, err error) {
		safeURI, err := url.ParseRequestURI(in)
		if err != nil {
			return "", err
		}
		return safeURI.String(), nil
	}

	if isHTTPS {
		scheme = "https:"
	} else {
		scheme = "http:"
	}

	switch len(parts) {
	case 4:
		rest = parts[3]
		fallthrough
	case 3:
		ns = parts[1]
		rootID = parts[2]
	default:
		return "", nil
	}

	if !isSubdomainNamespace(ns) {
		return "", nil
	}

	// add prefix if query is present
	if query != "" {
		query = "?" + query
	}

	// Normalize problematic PeerIDs (eg. ed25519+identity) to CID representation
	if isPeerIDNamespace(ns) && !isDomainNameAndNotPeerID(rootID) {
		peerID, err := peer.Decode(rootID)
		// Note: PeerID CIDv1 with protobuf multicodec will fail, but we fix it
		// in the next block
		if err == nil {
			rootID = peer.ToCid(peerID).String()
		}
	}

	// If rootID is a CID, ensure it uses DNS-friendly text representation
	if rootCID, err := cid.Decode(rootID); err == nil {
		multicodec := rootCID.Type()
		var base mbase.Encoding = mbase.Base32

		// Normalizations specific to /ipns/{libp2p-key}
		if isPeerIDNamespace(ns) {
			// Using Base36 for /ipns/ for consistency
			// Context: https://github.com/ipfs/kubo/pull/7441#discussion_r452372828
			base = mbase.Base36

			// PeerIDs represented as CIDv1 are expected to have libp2p-key
			// multicodec (https://github.com/libp2p/specs/pull/209).
			// We ease the transition by fixing multicodec on the fly:
			// https://github.com/ipfs/kubo/issues/5287#issuecomment-492163929
			if multicodec != cid.Libp2pKey {
				multicodec = cid.Libp2pKey
			}
		}

		// Ensure CID text representation used in subdomain is compatible
		// with the way DNS and URIs are implemented in user agents.
		//
		// 1. Switch to CIDv1 and enable case-insensitive Base encoding
		//    to avoid issues when user agent force-lowercases the hostname
		//    before making the request
		//    (https://github.com/ipfs/in-web-browsers/issues/89)
		rootCID = cid.NewCidV1(multicodec, rootCID.Hash())
		rootID, err = rootCID.StringOfBase(base)
		if err != nil {
			return "", err
		}
		// 2. Make sure CID fits in a DNS label, adjust encoding if needed
		//    (https://github.com/ipfs/kubo/issues/7318)
		rootID, err = toDNSLabel(rootID, rootCID)
		if err != nil {
			return "", err
		}
	} else { // rootID is not a CID

		// Check if rootID is a FQDN with DNSLink and convert it to TLS-safe
		// representation that fits in a single DNS label.  We support this so
		// loading DNSLink names over TLS "just works" on public HTTP gateways
		// that pass 'https' in X-Forwarded-Proto to go-ipfs.
		//
		// Rationale can be found under "Option C"
		// at: https://github.com/ipfs/in-web-browsers/issues/169
		//
		// TLDR is:
		// /ipns/my.v-long.example.com
		// can be loaded from a subdomain gateway with a wildcard TLS cert if
		// represented as a single DNS label:
		// https://my-v--long-example-com.ipns.dweb.link
		if (inlineDNSLink || isHTTPS) && ns == "ipns" && strings.Contains(rootID, ".") {
			if hasDNSLinkRecord(r.Context(), api, rootID) {
				// my.v-long.example.com → my-v--long-example-com
				dnsLabel, err := toDNSLinkDNSLabel(rootID)
				if err != nil {
					return "", err
				}
				// update path prefix to use real FQDN with DNSLink
				rootID = dnsLabel
			}
		}
	}

	return safeRedirectURL(fmt.Sprintf(
		"%s//%s.%s.%s/%s%s",
		scheme,
		rootID,
		ns,
		hostname,
		rest,
		query,
	))
}

func hasPrefix(path string, prefixes ...string) bool {
	for _, prefix := range prefixes {
		// Assume people are creative with trailing slashes in Gateway config
		p := strings.TrimSuffix(prefix, "/")
		// Support for both /version and /ipfs/$cid
		if p == path || strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	return false
}

func stripPort(hostname string) string {
	host, _, err := net.SplitHostPort(hostname)
	if err == nil {
		return host
	}
	return hostname
}

type hostnameGateways struct {
	exact    map[string]*Specification
	wildcard map[*regexp.Regexp]*Specification
}

// prepareHostnameGateways converts the user given gateways into an internal format
// split between exact and wildcard-based gateway hostnames.
func prepareHostnameGateways(gateways map[string]*Specification) *hostnameGateways {
	h := &hostnameGateways{
		exact:    map[string]*Specification{},
		wildcard: map[*regexp.Regexp]*Specification{},
	}

	for hostname, gw := range gateways {
		if strings.Contains(hostname, "*") {
			// from *.domain.tld, construct a regexp that match any direct subdomain
			// of .domain.tld.
			//
			// Regexp will be in the form of ^[^.]+\.domain.tld(?::\d+)?$
			escaped := strings.ReplaceAll(hostname, ".", `\.`)
			regexed := strings.ReplaceAll(escaped, "*", "[^.]+")

			re, err := regexp.Compile(fmt.Sprintf(`^%s(?::\d+)?$`, regexed))
			if err != nil {
				log.Warn("invalid wildcard gateway hostname \"%s\"", hostname)
			}

			h.wildcard[re] = gw
		} else {
			h.exact[hostname] = gw
		}
	}

	return h
}

// isKnownHostname checks the given hostname gateways and returns a matching
// specification with graceful fallback to version without port.
func (gws *hostnameGateways) isKnownHostname(hostname string) (gw *Specification, ok bool) {
	// Try hostname (host+optional port - value from Host header as-is)
	if gw, ok := gws.exact[hostname]; ok {
		return gw, ok
	}
	// Also test without port
	if gw, ok = gws.exact[stripPort(hostname)]; ok {
		return gw, ok
	}

	// Wildcard support. Test both with and without port.
	for re, spec := range gws.wildcard {
		if re.MatchString(hostname) {
			return spec, true
		}
	}

	return nil, false
}

// knownSubdomainDetails parses the Host header and looks for a known gateway matching
// the subdomain host. If found, returns a Specification and the subdomain components
// extracted from Host header: {rootID}.{ns}.{gwHostname}.
// Note: hostname is host + optional port
func (gws *hostnameGateways) knownSubdomainDetails(hostname string) (gw *Specification, gwHostname, ns, rootID string, ok bool) {
	labels := strings.Split(hostname, ".")
	// Look for FQDN of a known gateway hostname.
	// Example: given "dist.ipfs.tech.ipns.dweb.link":
	// 1. Lookup "link" TLD in knownGateways: negative
	// 2. Lookup "dweb.link" in knownGateways: positive
	//
	// Stops when we have 2 or fewer labels left as we need at least a
	// rootId and a namespace.
	for i := len(labels) - 1; i >= 2; i-- {
		fqdn := strings.Join(labels[i:], ".")
		gw, ok := gws.isKnownHostname(fqdn)
		if !ok {
			continue
		}

		ns := labels[i-1]
		if !isSubdomainNamespace(ns) {
			continue
		}

		// Merge remaining labels (could be a FQDN with DNSLink)
		rootID := strings.Join(labels[:i-1], ".")
		return gw, fqdn, ns, rootID, true
	}
	// no match
	return nil, "", "", "", false
}
