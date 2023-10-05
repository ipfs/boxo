package namesys

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	opts "github.com/ipfs/boxo/coreiface/options/namesys"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	offroute "github.com/ipfs/boxo/routing/offline"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	record "github.com/libp2p/go-libp2p-record"
	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	"github.com/stretchr/testify/assert"
)

type mockResolver struct {
	entries map[string]string
}

func testResolution(t *testing.T, resolver Resolver, name string, depth uint, expected string, expError error) {
	t.Helper()
	p, err := resolver.Resolve(context.Background(), name, opts.Depth(depth))
	if !errors.Is(err, expError) {
		t.Fatal(fmt.Errorf(
			"expected %s with a depth of %d to have a '%s' error, but got '%s'",
			name, depth, expError, err))
	}
	if expected == "" {
		assert.Nil(t, p, "%s with depth %d", name, depth)
	} else {
		assert.Equal(t, p.String(), expected, "%s with depth %d", name, depth)
	}
}

func (r *mockResolver) resolveOnceAsync(ctx context.Context, name string, options opts.ResolveOpts) <-chan onceResult {
	p, err := path.NewPath(r.entries[name])
	out := make(chan onceResult, 1)
	out <- onceResult{value: p, err: err}
	close(out)
	return out
}

func mockResolverOne() *mockResolver {
	return &mockResolver{
		entries: map[string]string{
			"QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy":              "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj",
			"QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n":              "/ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy",
			"QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD":              "/ipns/ipfs.io",
			"QmQ4QZh8nrsczdUEwTyfBope4THUhqxqc1fx6qYhhzZQei":              "/ipfs/QmP3ouCnU8NNLsW6261pAx2pNLV2E4dQoisB1sgda12Act",
			"12D3KooWFB51PRY9BxcXSH6khFXw1BZeszeLDy7C8GciskqCTZn5":        "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", // ed25519+identity multihash
			"bafzbeickencdqw37dpz3ha36ewrh4undfjt2do52chtcky4rxkj447qhdm": "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", // cidv1 in base32 with libp2p-key multicodec
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
	testResolution(t, r, "/ipns/12D3KooWFB51PRY9BxcXSH6khFXw1BZeszeLDy7C8GciskqCTZn5", 1, "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", ErrResolveRecursion)
	testResolution(t, r, "/ipns/bafzbeickencdqw37dpz3ha36ewrh4undfjt2do52chtcky4rxkj447qhdm", 1, "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", ErrResolveRecursion)
}

func TestPublishWithCache0(t *testing.T) {
	dst := dssync.MutexWrap(ds.NewMapDatastore())
	priv, _, err := ci.GenerateKeyPair(ci.RSA, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	err = ps.AddPrivKey(pid, priv)
	if err != nil {
		t.Fatal(err)
	}

	routing := offroute.NewOfflineRouter(dst, record.NamespacedValidator{
		"ipns": ipns.Validator{KeyBook: ps},
		"pk":   record.PublicKeyValidator{},
	})

	nsys, err := NewNameSystem(routing, WithDatastore(dst))
	if err != nil {
		t.Fatal(err)
	}

	// CID is arbitrary.
	p, err := path.NewPath("/ipfs/QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn")
	if err != nil {
		t.Fatal(err)
	}
	err = nsys.Publish(context.Background(), priv, p)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPublishWithTTL(t *testing.T) {
	dst := dssync.MutexWrap(ds.NewMapDatastore())
	priv, _, err := ci.GenerateKeyPair(ci.RSA, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	err = ps.AddPrivKey(pid, priv)
	if err != nil {
		t.Fatal(err)
	}

	routing := offroute.NewOfflineRouter(dst, record.NamespacedValidator{
		"ipns": ipns.Validator{KeyBook: ps},
		"pk":   record.PublicKeyValidator{},
	})

	nsys, err := NewNameSystem(routing, WithDatastore(dst), WithCache(128))
	if err != nil {
		t.Fatal(err)
	}

	// CID is arbitrary.
	p, err := path.NewPath("/ipfs/QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn")
	if err != nil {
		t.Fatal(err)
	}

	ttl := 1 * time.Second
	eol := time.Now().Add(2 * time.Second)

	err = nsys.Publish(context.Background(), priv, p, opts.PublishWithEOL(eol), opts.PublishWithTTL(ttl))
	if err != nil {
		t.Fatal(err)
	}
	ientry, ok := nsys.(*mpns).cache.Get(string(pid))
	if !ok {
		t.Fatal("cache get failed")
	}
	entry, ok := ientry.(cacheEntry)
	if !ok {
		t.Fatal("bad cache item returned")
	}
	if entry.eol.Sub(eol) > 10*time.Millisecond {
		t.Fatalf("bad cache ttl: expected %s, got %s", eol, entry.eol)
	}
}
