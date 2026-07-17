package namesys

import (
	"context"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	offroute "github.com/ipfs/boxo/routing/offline"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	record "github.com/libp2p/go-libp2p-record"
	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

type mockResolver struct {
	entries map[string]string
}

func testResolution(t *testing.T, resolver Resolver, name string, depth uint, expected string, expectedTTL time.Duration, expectedError error) {
	t.Helper()

	ptr, err := path.NewPath(name)
	require.NoError(t, err)

	res, err := resolver.Resolve(context.Background(), ptr, ResolveWithDepth(depth))
	require.ErrorIs(t, err, expectedError)
	require.Equal(t, expectedTTL, res.TTL)
	if expected == "" {
		require.Nil(t, res.Path, "%s with depth %d", name, depth)
	} else {
		require.Equal(t, expected, res.Path.String(), "%s with depth %d", name, depth)
	}
}

func (r *mockResolver) resolveOnceAsync(ctx context.Context, p path.Path, options ResolveOptions) <-chan AsyncResult {
	p, err := path.NewPath(r.entries[p.String()])
	out := make(chan AsyncResult, 1)
	out <- AsyncResult{Path: p, Err: err}
	close(out)
	return out
}

func mockResolverOne() *mockResolver {
	return &mockResolver{
		entries: map[string]string{
			"/ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy":              "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj",
			"/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n":              "/ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy",
			"/ipns/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD":              "/ipns/ipfs.io",
			"/ipns/QmQ4QZh8nrsczdUEwTyfBope4THUhqxqc1fx6qYhhzZQei":              "/ipfs/QmP3ouCnU8NNLsW6261pAx2pNLV2E4dQoisB1sgda12Act",
			"/ipns/12D3KooWFB51PRY9BxcXSH6khFXw1BZeszeLDy7C8GciskqCTZn5":        "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", // ed25519+identity multihash
			"/ipns/bafzbeickencdqw37dpz3ha36ewrh4undfjt2do52chtcky4rxkj447qhdm": "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", // cidv1 in base32 with libp2p-key multicodec
		},
	}
}

func mockResolverTwo() *mockResolver {
	return &mockResolver{
		entries: map[string]string{
			"/ipns/ipfs.io": "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n",
		},
	}
}

func TestNamesysResolution(t *testing.T) {
	r := &namesys{
		ipnsResolver: mockResolverOne(),
		dnsResolver:  mockResolverTwo(),
	}

	for _, testCase := range []struct {
		name          string
		depth         uint
		expectedPath  string
		expectedTTL   time.Duration
		expectedError error
	}{
		{"/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj", DefaultDepthLimit, "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj", 0, nil},
		{"/ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy", DefaultDepthLimit, "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj", 0, nil},
		{"/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", DefaultDepthLimit, "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj", 0, nil},
		{"/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", 1, "/ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy", 0, ErrResolveRecursion},
		{"/ipns/ipfs.io", DefaultDepthLimit, "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj", 0, nil},
		{"/ipns/ipfs.io", 1, "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", 0, ErrResolveRecursion},
		{"/ipns/ipfs.io", 2, "/ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy", 0, ErrResolveRecursion},
		{"/ipns/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", DefaultDepthLimit, "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj", 0, nil},
		{"/ipns/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", 1, "/ipns/ipfs.io", 0, ErrResolveRecursion},
		{"/ipns/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", 2, "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", 0, ErrResolveRecursion},
		{"/ipns/QmY3hE8xgFCjGcz6PHgnvJz5HZi1BaKRfPkn1ghZUcYMjD", 3, "/ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy", 0, ErrResolveRecursion},
		{"/ipns/12D3KooWFB51PRY9BxcXSH6khFXw1BZeszeLDy7C8GciskqCTZn5", 1, "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", 0, ErrResolveRecursion},
		{"/ipns/bafzbeickencdqw37dpz3ha36ewrh4undfjt2do52chtcky4rxkj447qhdm", 1, "/ipns/QmbCMUZw6JFeZ7Wp9jkzbye3Fzp2GGcPgC3nmeUjfVF87n", 0, ErrResolveRecursion},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			testResolution(t, r, testCase.name, (testCase.depth), testCase.expectedPath, 0, testCase.expectedError)
		})
	}
}

func TestResolveIPNS(t *testing.T) {
	ns := &namesys{
		ipnsResolver: mockResolverOne(),
		dnsResolver:  mockResolverTwo(),
	}

	inputPath, err := path.NewPath("/ipns/QmatmE9msSfkKxoffpHwNLNKgwZG8eT9Bud6YoPab52vpy/a/b/c")
	require.NoError(t, err)

	res, err := Resolve(context.Background(), ns, inputPath)
	require.NoError(t, err)
	require.Equal(t, "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj/a/b/c", res.Path.String())
}

func TestPublishWithCache0(t *testing.T) {
	dst := dssync.MutexWrap(ds.NewMapDatastore())
	priv, _, err := ci.GenerateKeyPair(ci.RSA, 4096)
	require.NoError(t, err)

	routing := offroute.NewOfflineRouter(dst, record.NamespacedValidator{
		"ipns": ipns.Validator{}, // No need for KeyBook, as records created by NameSys include PublicKey for RSA.
		"pk":   record.PublicKeyValidator{},
	})

	nsys, err := NewNameSystem(routing, WithDatastore(dst))
	require.NoError(t, err)

	// CID is arbitrary.
	p, err := path.NewPath("/ipfs/QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn")
	require.NoError(t, err)

	err = nsys.Publish(context.Background(), priv, p)
	require.NoError(t, err)
}

func TestPublishWithTTL(t *testing.T) {
	dst := dssync.MutexWrap(ds.NewMapDatastore())
	priv, _, err := ci.GenerateKeyPair(ci.RSA, 2048)
	require.NoError(t, err)

	pid, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)

	routing := offroute.NewOfflineRouter(dst, record.NamespacedValidator{
		"ipns": ipns.Validator{}, // No need for KeyBook, as records created by NameSys include PublicKey for RSA.
		"pk":   record.PublicKeyValidator{},
	})

	// CID is arbitrary.
	p, err := path.NewPath("/ipfs/QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn")
	require.NoError(t, err)

	ttl := 5 * time.Minute
	eol := time.Now().Add(time.Hour)

	t.Run("Without MaxCacheTTL", func(t *testing.T) {
		ns, err := NewNameSystem(routing, WithDatastore(dst), WithCache(128))
		require.NoError(t, err)

		err = ns.Publish(context.Background(), priv, p, PublishWithEOL(eol), PublishWithTTL(ttl))
		require.NoError(t, err)

		entry, ok := ns.(*namesys).cache.Get(ipns.NameFromPeer(pid).String())
		require.True(t, ok)
		require.Equal(t, ttl, entry.ttl)
		require.LessOrEqual(t, time.Until(entry.cacheEOL), ttl)
	})

	t.Run("With MaxCacheTTL", func(t *testing.T) {
		cacheTTL := 30 * time.Second

		ns, err := NewNameSystem(routing, WithDatastore(dst), WithCache(128), WithMaxCacheTTL(cacheTTL))
		require.NoError(t, err)

		err = ns.Publish(context.Background(), priv, p, PublishWithEOL(eol), PublishWithTTL(ttl))
		require.NoError(t, err)

		entry, ok := ns.(*namesys).cache.Get(ipns.NameFromPeer(pid).String())
		require.True(t, ok)
		require.Equal(t, ttl, entry.ttl)
		require.LessOrEqual(t, time.Until(entry.cacheEOL), cacheTTL)
	})
}

func TestResolveMaxCacheTTLCapsResultTTL(t *testing.T) {
	t.Parallel()

	const recordTTL = time.Hour
	const expectedPath = "/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj"
	lookup := func(ctx context.Context, name string) ([]string, time.Duration, error) {
		return []string{"dnslink=" + expectedPath}, recordTTL, nil
	}

	// fresh resolutions report the TTL capped by WithMaxCacheTTL
	maxTTL := 30 * time.Second
	ns := &namesys{dnsResolver: NewDNSResolverWithTTL(lookup), maxCacheTTL: &maxTTL}
	testResolution(t, ns, "/ipns/example.com", DefaultDepthLimit, expectedPath, maxTTL, nil)

	// without the cap the record TTL flows through
	nsNoCap := &namesys{dnsResolver: NewDNSResolverWithTTL(lookup)}
	testResolution(t, nsNoCap, "/ipns/example.com", DefaultDepthLimit, expectedPath, recordTTL, nil)

	// a non-positive cap disables the cache but must not touch the reported
	// TTL: kubo's offline node passes 0 and still expects Cache-Control
	// max-age derived from the record TTL
	zeroTTL := time.Duration(0)
	nsZero := &namesys{dnsResolver: NewDNSResolverWithTTL(lookup), maxCacheTTL: &zeroTTL}
	testResolution(t, nsZero, "/ipns/example.com", DefaultDepthLimit, expectedPath, recordTTL, nil)

	negTTL := -time.Second
	nsNeg := &namesys{dnsResolver: NewDNSResolverWithTTL(lookup), maxCacheTTL: &negTTL}
	testResolution(t, nsNeg, "/ipns/example.com", DefaultDepthLimit, expectedPath, recordTTL, nil)
}

func TestCacheGetReportsRemainingTTL(t *testing.T) {
	t.Parallel()

	p, err := path.NewPath("/ipfs/Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj")
	require.NoError(t, err)

	const name = "/ipns/example.com"
	const entryTTL = 5 * time.Minute

	ns := &namesys{}
	require.NoError(t, WithCache(128)(ns))
	ns.cacheSet(name, p, entryTTL, time.Now())

	// a hit reports the entry's remaining lifetime, never more than its TTL
	_, ttl, _, ok := ns.cacheGet(name)
	require.True(t, ok)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, entryTTL)

	// age the entry artificially and confirm the reported TTL shrinks with it
	entry, ok := ns.cache.Get(name)
	require.True(t, ok)
	entry.cacheEOL = time.Now().Add(5 * time.Second)
	ns.cache.Add(name, entry)

	_, ttl, _, ok = ns.cacheGet(name)
	require.True(t, ok)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, 5*time.Second)

	// a cap set via WithMaxCacheTTL bounds the reported TTL as well
	maxTTL := 30 * time.Second
	nsCapped := &namesys{maxCacheTTL: &maxTTL}
	require.NoError(t, WithCache(128)(nsCapped))
	nsCapped.cacheSet(name, p, entryTTL, time.Now())

	_, ttl, _, ok = nsCapped.cacheGet(name)
	require.True(t, ok)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, maxTTL)

	// a cap of 0 disables retention: nothing is served from the cache
	zeroTTL := time.Duration(0)
	nsZero := &namesys{maxCacheTTL: &zeroTTL}
	require.NoError(t, WithCache(128)(nsZero))
	nsZero.cacheSet(name, p, entryTTL, time.Now())

	_, _, _, ok = nsZero.cacheGet(name)
	require.False(t, ok)
}

func TestCacheGetClampsTTLToCacheEOL(t *testing.T) {
	t.Parallel()

	cache, err := lru.New[string, cacheEntry](8)
	require.NoError(t, err)
	ns := &namesys{cache: cache}

	p, err := path.NewPath("/ipfs/QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn")
	require.NoError(t, err)

	t.Run("late hit clamps ttl to remaining cache lifetime", func(t *testing.T) {
		// Original TTL is an hour, but the cache entry is about to expire: the
		// returned TTL must not outlive the (EOL-bounded) cache lifetime.
		ns.cache.Add("/ipns/late", cacheEntry{
			val:      p,
			ttl:      time.Hour,
			cacheEOL: time.Now().Add(2 * time.Second),
			lastMod:  time.Now(),
		})
		_, ttl, _, ok := ns.cacheGet("/ipns/late")
		require.True(t, ok)
		require.Greater(t, ttl, time.Duration(0))
		require.LessOrEqual(t, ttl, 2*time.Second)
	})

	t.Run("early hit returns full ttl", func(t *testing.T) {
		ns.cache.Add("/ipns/early", cacheEntry{
			val:      p,
			ttl:      30 * time.Second,
			cacheEOL: time.Now().Add(time.Hour),
			lastMod:  time.Now(),
		})
		_, ttl, _, ok := ns.cacheGet("/ipns/early")
		require.True(t, ok)
		require.Equal(t, 30*time.Second, ttl)
	})
}
