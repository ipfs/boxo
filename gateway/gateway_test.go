package gateway

import (
	"context"
	"errors"
	"io"
	"strings"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/go-namesys"
	path "github.com/ipfs/go-path"
	nsopts "github.com/ipfs/interface-go-ipfs-core/options/namesys"
	ipath "github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/libp2p/go-libp2p/core/crypto"
)

type mockNamesys map[string]path.Path

func (m mockNamesys) Resolve(ctx context.Context, name string, opts ...nsopts.ResolveOpt) (value path.Path, err error) {
	cfg := nsopts.DefaultResolveOpts()
	for _, o := range opts {
		o(&cfg)
	}
	depth := cfg.Depth
	if depth == nsopts.UnlimitedDepth {
		// max uint
		depth = ^uint(0)
	}
	for strings.HasPrefix(name, "/ipns/") {
		if depth == 0 {
			return value, namesys.ErrResolveRecursion
		}
		depth--

		var ok bool
		value, ok = m[name]
		if !ok {
			return "", namesys.ErrResolveFailed
		}
		name = value.String()
	}
	return value, nil
}

func (m mockNamesys) ResolveAsync(ctx context.Context, name string, opts ...nsopts.ResolveOpt) <-chan namesys.Result {
	out := make(chan namesys.Result, 1)
	v, err := m.Resolve(ctx, name, opts...)
	out <- namesys.Result{Path: v, Err: err}
	close(out)
	return out
}

func (m mockNamesys) Publish(ctx context.Context, name crypto.PrivKey, value path.Path, opts ...nsopts.PublishOption) error {
	return errors.New("not implemented for mockNamesys")
}

func (m mockNamesys) GetResolver(subs string) (namesys.Resolver, bool) {
	return nil, false
}

type mockApi struct {
	ns mockNamesys
}

var _ API = (*mockApi)(nil)

func newMockApi() *mockApi {
	return &mockApi{
		ns: mockNamesys{},
	}
}

func (m *mockApi) Get(ctx context.Context, immutablePath ImmutablePath, opt ...GetOpt) (GatewayMetadata, files.Node, error) {
	return GatewayMetadata{}, nil, errors.New("not implemented")
}

func (m *mockApi) Head(ctx context.Context, immutablePath ImmutablePath) (GatewayMetadata, files.Node, error) {
	return GatewayMetadata{}, nil, errors.New("not implemented")
}

func (m *mockApi) GetCAR(ctx context.Context, immutablePath ImmutablePath) (GatewayMetadata, io.ReadCloser, error, <-chan error) {
	return GatewayMetadata{}, nil, nil, errors.New("not implemented")
}

func (m *mockApi) ResolveMutable(ctx context.Context, p ipath.Path) (ImmutablePath, error) {
	return ImmutablePath{}, errors.New("not implemented")
}

func (m *mockApi) GetIPNSRecord(context.Context, cid.Cid) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (m *mockApi) GetDNSLinkRecord(ctx context.Context, hostname string) (ipath.Path, error) {
	p, err := m.ns.Resolve(ctx, "/ipns/"+hostname, nsopts.Depth(1))
	if err == namesys.ErrResolveRecursion {
		err = nil
	}
	return ipath.New(p.String()), err
}

func (m *mockApi) IsCached(context.Context, ipath.Path) bool {
	return false
}
