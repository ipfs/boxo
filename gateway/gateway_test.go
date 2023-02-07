package gateway

import (
	"context"
	"errors"
	"strings"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/files"
	"github.com/ipfs/go-namesys"
	path "github.com/ipfs/go-path"
	iface "github.com/ipfs/interface-go-ipfs-core"
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

func newMockApi() *mockApi {
	return &mockApi{
		ns: mockNamesys{},
	}
}

func (m *mockApi) GetUnixFsNode(context.Context, ipath.Resolved) (files.Node, error) {
	return nil, errors.New("not implemented")
}

func (m *mockApi) LsUnixFsDir(context.Context, ipath.Resolved) (<-chan iface.DirEntry, error) {
	return nil, errors.New("not implemented")
}

func (m *mockApi) GetBlock(context.Context, cid.Cid) (blocks.Block, error) {
	return nil, errors.New("not implemented")
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

func (m *mockApi) ResolvePath(context.Context, ipath.Path) (ipath.Resolved, error) {
	return nil, errors.New("not implemented")
}
