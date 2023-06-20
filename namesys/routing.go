package namesys

import (
	"context"
	"strings"
	"time"

	opts "github.com/ipfs/boxo/coreiface/options/namesys"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	logging "github.com/ipfs/go-log/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var log = logging.Logger("namesys")

// IpnsResolver implements NSResolver for the main IPFS SFS-like naming
type IpnsResolver struct {
	routing routing.ValueStore
}

// NewIpnsResolver constructs a name resolver using the IPFS Routing system
// to implement SFS-like naming on top.
func NewIpnsResolver(route routing.ValueStore) *IpnsResolver {
	if route == nil {
		panic("attempt to create resolver with nil routing system")
	}
	return &IpnsResolver{
		routing: route,
	}
}

// Resolve implements Resolver.
func (r *IpnsResolver) Resolve(ctx context.Context, name string, options ...opts.ResolveOpt) (path.Path, error) {
	ctx, span := StartSpan(ctx, "IpnsResolver.Resolve", trace.WithAttributes(attribute.String("Name", name)))
	defer span.End()
	return resolve(ctx, r, name, opts.ProcessOpts(options))
}

// ResolveAsync implements Resolver.
func (r *IpnsResolver) ResolveAsync(ctx context.Context, name string, options ...opts.ResolveOpt) <-chan Result {
	ctx, span := StartSpan(ctx, "IpnsResolver.ResolveAsync", trace.WithAttributes(attribute.String("Name", name)))
	defer span.End()
	return resolveAsync(ctx, r, name, opts.ProcessOpts(options))
}

// resolveOnce implements resolver. Uses the IPFS routing system to
// resolve SFS-like names.
func (r *IpnsResolver) resolveOnceAsync(ctx context.Context, name string, options opts.ResolveOpts) <-chan onceResult {
	ctx, span := StartSpan(ctx, "IpnsResolver.ResolveOnceAsync", trace.WithAttributes(attribute.String("Name", name)))
	defer span.End()

	out := make(chan onceResult, 1)
	log.Debugf("RoutingResolver resolving %s", name)
	cancel := func() {}

	if options.DhtTimeout != 0 {
		// Resolution must complete within the timeout
		ctx, cancel = context.WithTimeout(ctx, options.DhtTimeout)
	}

	name = strings.TrimPrefix(name, "/ipns/")

	pid, err := peer.Decode(name)
	if err != nil {
		log.Debugf("RoutingResolver: could not convert public key hash %s to peer ID: %s\n", name, err)
		out <- onceResult{err: err}
		close(out)
		cancel()
		return out
	}

	// Use the routing system to get the name.
	// Note that the DHT will call the ipns validator when retrieving
	// the value, which in turn verifies the ipns record signature
	ipnsKey := string(ipns.NameFromPeer(pid).RoutingKey())

	vals, err := r.routing.SearchValue(ctx, ipnsKey, dht.Quorum(int(options.DhtRecordCount)))
	if err != nil {
		log.Debugf("RoutingResolver: dht get for name %s failed: %s", name, err)
		out <- onceResult{err: err}
		close(out)
		cancel()
		return out
	}

	go func() {
		defer cancel()
		defer close(out)
		ctx, span := StartSpan(ctx, "IpnsResolver.ResolveOnceAsync.Worker")
		defer span.End()

		for {
			select {
			case val, ok := <-vals:
				if !ok {
					return
				}

				rec, err := ipns.UnmarshalRecord(val)
				if err != nil {
					log.Debugf("RoutingResolver: could not unmarshal value for name %s: %s", name, err)
					emitOnceResult(ctx, out, onceResult{err: err})
					return
				}

				p, err := rec.Value()
				if err != nil {
					emitOnceResult(ctx, out, onceResult{err: err})
					return
				}

				ttl := DefaultResolverCacheTTL
				if recordTTL, err := rec.TTL(); err == nil {
					ttl = recordTTL
				}

				switch eol, err := rec.Validity(); err {
				case ipns.ErrUnrecognizedValidity:
					// No EOL.
				case nil:
					ttEol := time.Until(eol)
					if ttEol < 0 {
						// It *was* valid when we first resolved it.
						ttl = 0
					} else if ttEol < ttl {
						ttl = ttEol
					}
				default:
					log.Errorf("encountered error when parsing EOL: %s", err)
					emitOnceResult(ctx, out, onceResult{err: err})
					return
				}

				emitOnceResult(ctx, out, onceResult{value: path.Path(p.String()), ttl: ttl})
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}
