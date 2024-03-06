package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	routinghelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/routing"
)

// baseBackend contains some common backend functionalities that are shared by
// different backend implementations.
type baseBackend struct {
	routing routing.ValueStore
	namesys namesys.NameSystem
}

func newBaseBackend(vs routing.ValueStore, ns namesys.NameSystem) (baseBackend, error) {
	if vs == nil {
		vs = routinghelpers.Null{}
	}

	if ns == nil {
		dns, err := NewDNSResolver(nil, nil)
		if err != nil {
			return baseBackend{}, err
		}

		ns, err = namesys.NewNameSystem(vs, namesys.WithDNSResolver(dns))
		if err != nil {
			return baseBackend{}, err
		}
	}

	return baseBackend{
		routing: vs,
		namesys: ns,
	}, nil
}

func (bb *baseBackend) ResolveMutable(ctx context.Context, p path.Path) (path.ImmutablePath, time.Duration, time.Time, error) {
	switch p.Namespace() {
	case path.IPNSNamespace:
		res, err := namesys.Resolve(ctx, bb.namesys, p)
		if err != nil {
			return path.ImmutablePath{}, 0, time.Time{}, err
		}
		ip, err := path.NewImmutablePath(res.Path)
		if err != nil {
			return path.ImmutablePath{}, 0, time.Time{}, err
		}
		return ip, res.TTL, res.LastMod, nil
	case path.IPFSNamespace:
		ip, err := path.NewImmutablePath(p)
		return ip, 0, time.Time{}, err
	default:
		return path.ImmutablePath{}, 0, time.Time{}, NewErrorStatusCode(fmt.Errorf("unsupported path namespace: %s", p.Namespace()), http.StatusNotImplemented)
	}
}

func (bb *baseBackend) GetIPNSRecord(ctx context.Context, c cid.Cid) ([]byte, error) {
	if bb.routing == nil {
		return nil, NewErrorStatusCode(errors.New("IPNS Record responses are not supported by this gateway"), http.StatusNotImplemented)
	}

	name, err := ipns.NameFromCid(c)
	if err != nil {
		return nil, NewErrorStatusCode(err, http.StatusBadRequest)
	}

	return bb.routing.GetValue(ctx, string(name.RoutingKey()))
}

func (bb *baseBackend) GetDNSLinkRecord(ctx context.Context, hostname string) (path.Path, error) {
	if bb.namesys != nil {
		p, err := path.NewPath("/ipns/" + hostname)
		if err != nil {
			return nil, err
		}
		res, err := bb.namesys.Resolve(ctx, p, namesys.ResolveWithDepth(1))
		if err == namesys.ErrResolveRecursion {
			err = nil
		}
		return res.Path, err
	}

	return nil, NewErrorStatusCode(errors.New("not implemented"), http.StatusNotImplemented)
}
