package namesys

import (
	"context"
	"fmt"
	"testing"

	pstoremem "gx/ipfs/QmRhFARzTHcFh8wUxwN5KvyTGq73FLC65EfFAhz8Ng7aGb/go-libp2p-peerstore/pstoremem"
	ci "gx/ipfs/QmTW4SdgBWq9GjsBsHeUx8WuGxzhgzAf88UMH2w62PC8yK/go-libp2p-crypto"
	peer "gx/ipfs/QmTu65MVbemtUxJEWgsTtzv9Zv9P8rvmqNA4eG9TrTRGYc/go-libp2p-peer"
	opts "gx/ipfs/QmUM3JbzMPPVpsUvUcfCdmeU2tssrdVPnUn5E6RawFjDLC/interface-go-ipfs-core/options/namesys"
	ds "gx/ipfs/QmUadX5EcvrBmxAV9sE7wUWtWSqxns5K84qKJBixmcT1w9/go-datastore"
	dssync "gx/ipfs/QmUadX5EcvrBmxAV9sE7wUWtWSqxns5K84qKJBixmcT1w9/go-datastore/sync"
	path "gx/ipfs/QmaVydE6qtiiMhRUraPrzL6dvtfHPNR6Y9JJZcTwDAw9dY/go-path"
	offroute "gx/ipfs/QmcjqHcsk8E1Gd8RbuaUawWC7ogDtaVcdjLvZF8ysCCiPn/go-ipfs-routing/offline"
	ipns "gx/ipfs/QmdboayjE53q27kq6zGk5vkx4u7LDGdcbVoi5NXMCtfiKS/go-ipns"
	"gx/ipfs/QmfKmRac17N7UmsV16vifHtB5Eqz3hTyKcu22Qb2oFoZyR/go-unixfs"
)

type mockResolver struct {
	entries map[string]string
}

func testResolution(t *testing.T, resolver Resolver, name string, depth uint, expected string, expError error) {
	t.Helper()
	p, err := resolver.Resolve(context.Background(), name, opts.Depth(depth))
	if err != expError {
		t.Fatal(fmt.Errorf(
			"expected %s with a depth of %d to have a '%s' error, but got '%s'",
			name, depth, expError, err))
	}
	if p.String() != expected {
		t.Fatal(fmt.Errorf(
			"%s with depth %d resolved to %s != %s",
			name, depth, p.String(), expected))
	}
}

func (r *mockResolver) resolveOnceAsync(ctx context.Context, name string, options opts.ResolveOpts) <-chan onceResult {
	p, err := path.ParsePath(r.entries[name])
	out := make(chan onceResult, 1)
	out <- onceResult{value: p, err: err}
	close(out)
	return out
}

func mockResolverOne() *mockResolver {
	return &mockResolver{
		entries: map[string]string{
			"QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy": "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj",
			"QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n": "/ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy",
			"QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD": "/ipns/ipfs.io",
		},
	}
}

func mockResolverTwo() *mockResolver {
	return &mockResolver{
		entries: map[string]string{
			"ipfs.io": "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n",
		},
	}
}

func TestNamesysResolution(t *testing.T) {
	r := &mpns{
		ipnsResolver: mockResolverOne(),
		dnsResolver:  mockResolverTwo(),
	}

	testResolution(t, r, "Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj", opts.DefaultDepthLimit, "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj", nil)
	testResolution(t, r, "/ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy", opts.DefaultDepthLimit, "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj", nil)
	testResolution(t, r, "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", opts.DefaultDepthLimit, "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj", nil)
	testResolution(t, r, "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", 1, "/ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy", ErrResolveRecursion)
	testResolution(t, r, "/ipns/ipfs.io", opts.DefaultDepthLimit, "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj", nil)
	testResolution(t, r, "/ipns/ipfs.io", 1, "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", ErrResolveRecursion)
	testResolution(t, r, "/ipns/ipfs.io", 2, "/ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy", ErrResolveRecursion)
	testResolution(t, r, "/ipns/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", opts.DefaultDepthLimit, "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj", nil)
	testResolution(t, r, "/ipns/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", 1, "/ipns/ipfs.io", ErrResolveRecursion)
	testResolution(t, r, "/ipns/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", 2, "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", ErrResolveRecursion)
	testResolution(t, r, "/ipns/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", 3, "/ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy", ErrResolveRecursion)
}

func TestPublishWithCache0(t *testing.T) {
	dst := dssync.MutexWrap(ds.NewMapDatastore())
	priv, _, err := ci.GenerateKeyPair(ci.RSA, 1024)
	if err != nil {
		t.Fatal(err)
	}
	ps := pstoremem.NewPeerstore()
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	err = ps.AddPrivKey(pid, priv)
	if err != nil {
		t.Fatal(err)
	}

	routing := offroute.NewOfflineRouter(dst, ipns.Validator{KeyBook: ps})

	nsys := NewNameSystem(routing, dst, 0)
	p, err := path.ParsePath(unixfs.EmptyDirNode().Cid().String())
	if err != nil {
		t.Fatal(err)
	}
	nsys.Publish(context.Background(), priv, p)
}
