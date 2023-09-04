package namesys

import (
	"context"
	"errors"
	"fmt"
	"net"
	gopath "path"
	"strings"
	"time"

	path "github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	dns "github.com/miekg/dns"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// LookupTXTFunc is a function that lookups TXT record values.
type LookupTXTFunc func(ctx context.Context, name string) (txt []string, err error)

// DNSResolver implements [Resolver] on DNS domains.
type DNSResolver struct {
	lookupTXT LookupTXTFunc
}

var _ Resolver = &DNSResolver{}

// NewDNSResolver constructs a name resolver using DNS TXT records.
func NewDNSResolver(lookup LookupTXTFunc) *DNSResolver {
	return &DNSResolver{lookupTXT: lookup}
}

func (r *DNSResolver) Resolve(ctx context.Context, p path.Path, options ...ResolveOption) (ResolveResult, error) {
	ctx, span := startSpan(ctx, "DNSResolver.Resolve", trace.WithAttributes(attribute.Stringer("Path", p)))
	defer span.End()

	return resolve(ctx, r, p, ProcessResolveOptions(options))
}

func (r *DNSResolver) ResolveAsync(ctx context.Context, p path.Path, options ...ResolveOption) <-chan ResolveAsyncResult {
	ctx, span := startSpan(ctx, "DNSResolver.ResolveAsync", trace.WithAttributes(attribute.Stringer("Path", p)))
	defer span.End()

	return resolveAsync(ctx, r, p, ProcessResolveOptions(options))
}

func (r *DNSResolver) resolveOnceAsync(ctx context.Context, p path.Path, options ResolveOptions) <-chan ResolveAsyncResult {
	ctx, span := startSpan(ctx, "DNSResolver.ResolveOnceAsync", trace.WithAttributes(attribute.Stringer("Path", p)))
	defer span.End()

	out := make(chan ResolveAsyncResult, 1)
	if p.Namespace() != path.IPNSNamespace {
		out <- ResolveAsyncResult{Err: fmt.Errorf("unsupported namespace: %s", p.Namespace())}
		close(out)
		return out
	}

	fqdn := p.Segments()[1]
	if _, ok := dns.IsDomainName(fqdn); !ok {
		out <- ResolveAsyncResult{Err: fmt.Errorf("not a valid domain name: %q", fqdn)}
		close(out)
		return out
	}

	log.Debugf("DNSResolver resolving %s", fqdn)

	if !strings.HasSuffix(fqdn, ".") {
		fqdn += "."
	}

	rootChan := make(chan ResolveAsyncResult, 1)
	go workDomain(ctx, r, fqdn, rootChan)

	subChan := make(chan ResolveAsyncResult, 1)
	go workDomain(ctx, r, "_dnslink."+fqdn, subChan)

	go func() {
		defer close(out)
		ctx, span := startSpan(ctx, "DNSResolver.ResolveOnceAsync.Worker")
		defer span.End()

		var rootResErr, subResErr error
		for {
			select {
			case subRes, ok := <-subChan:
				if !ok {
					subChan = nil
					break
				}
				if subRes.Err == nil {
					p, err := joinPaths(subRes.Path, p)
					emitOnceResult(ctx, out, ResolveAsyncResult{Path: p, LastMod: time.Now(), Err: err})
					// Return without waiting for rootRes, since this result
					// (for "_dnslink."+fqdn) takes precedence
					return
				}
				subResErr = subRes.Err
			case rootRes, ok := <-rootChan:
				if !ok {
					rootChan = nil
					break
				}
				if rootRes.Err == nil {
					p, err := joinPaths(rootRes.Path, p)
					emitOnceResult(ctx, out, ResolveAsyncResult{Path: p, LastMod: time.Now(), Err: err})
					// Do not return here.  Wait for subRes so that it is
					// output last if good, thereby giving subRes precedence.
				} else {
					rootResErr = rootRes.Err
				}
			case <-ctx.Done():
				return
			}
			if subChan == nil && rootChan == nil {
				// If here, then both lookups are done
				//
				// If both lookups failed due to no TXT records with a
				// dnslink, then output a more specific error message
				if rootResErr == ErrResolveFailed && subResErr == ErrResolveFailed {
					// Wrap error so that it can be tested if it is a ErrResolveFailed
					err := fmt.Errorf("%w: _dnslink subdomain at %q is missing a TXT record (https://docs.ipfs.tech/concepts/dnslink/)", ErrResolveFailed, gopath.Base(fqdn))
					emitOnceResult(ctx, out, ResolveAsyncResult{Err: err})
				}
				return
			}
		}
	}()

	return out
}

func workDomain(ctx context.Context, r *DNSResolver, name string, res chan ResolveAsyncResult) {
	ctx, span := startSpan(ctx, "DNSResolver.WorkDomain", trace.WithAttributes(attribute.String("Name", name)))
	defer span.End()

	defer close(res)

	txt, err := r.lookupTXT(ctx, name)
	if err != nil {
		if dnsErr, ok := err.(*net.DNSError); ok {
			// If no TXT records found, return same error as when no text
			// records contain dnslink. Otherwise, return the actual error.
			if dnsErr.IsNotFound {
				err = ErrResolveFailed
			}
		}
		// Could not look up any text records for name
		res <- ResolveAsyncResult{Err: err}
		return
	}

	for _, t := range txt {
		p, err := parseEntry(t)
		if err == nil {
			res <- ResolveAsyncResult{Path: p}
			return
		}
	}

	// There were no TXT records with a dnslink
	res <- ResolveAsyncResult{Err: ErrResolveFailed}
}

func parseEntry(txt string) (path.Path, error) {
	p, err := path.NewPath(txt) // bare IPFS multihashes
	if err == nil {
		return p, nil
	}

	// Support legacy DNSLink entries composed by the CID only.
	if cid, err := cid.Decode(txt); err == nil {
		return path.FromCid(cid), nil
	}

	return tryParseDNSLink(txt)
}

func tryParseDNSLink(txt string) (path.Path, error) {
	parts := strings.SplitN(txt, "=", 2)
	if len(parts) == 2 && parts[0] == "dnslink" {
		p, err := path.NewPath(parts[1])
		if err == nil {
			return p, nil
		}

		// Support legacy DNSLink entries composed by "dnslink={CID}".
		if cid, err := cid.Decode(parts[1]); err == nil {
			return path.FromCid(cid), nil
		}
	}

	return nil, errors.New("not a valid dnslink entry")
}
