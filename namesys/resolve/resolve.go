package resolve

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/boxo/path"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/boxo/namesys"
)

// ErrNoNamesys is an explicit error for when an IPFS node doesn't
// (yet) have a name system
var ErrNoNamesys = errors.New(
	"core/resolve: no Namesys on IpfsNode - can't resolve ipns entry")

// ResolveIPNS resolves /ipns paths
func ResolveIPNS(ctx context.Context, nsys namesys.NameSystem, p path.Path) (path.Path, error) {
	ctx, span := namesys.StartSpan(ctx, "ResolveIPNS", trace.WithAttributes(attribute.String("Path", p.String())))
	defer span.End()

	if p.Namespace() == path.IPNSNamespace {
		// TODO(cryptix): we should be able to query the local cache for the path
		if nsys == nil {
			return nil, ErrNoNamesys
		}

		seg := p.Segments()

		if len(seg) < 2 || seg[1] == "" { // just "/<protocol/>" without further segments
			err := fmt.Errorf("invalid path %q: ipns path missing IPNS ID", p)
			return nil, err
		}

		extensions := seg[2:]
		resolvable, err := path.NewPathFromSegments(seg[0], seg[1])
		if err != nil {
			return nil, err
		}

		respath, err := nsys.Resolve(ctx, resolvable.String())
		if err != nil {
			return nil, err
		}

		segments := append(respath.Segments(), extensions...)
		p, err = path.NewPathFromSegments(segments...)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}
