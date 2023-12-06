package namesys

import (
	"context"
	"testing"
	"time"

	ipns "github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/routing/offline"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	record "github.com/libp2p/go-libp2p-record"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/stretchr/testify/require"
)

type noFailValidator struct{}

func (v noFailValidator) Validate(key string, value []byte) error {
	return nil
}

func (v noFailValidator) Select(key string, values [][]byte) (int, error) {
	return 0, nil
}

func TestResolver(t *testing.T) {
	t.Parallel()

	pathCat := path.FromCid(cid.MustParse("bafkqabddmf2au"))
	pathDog := path.FromCid(cid.MustParse("bafkqabden5tqu"))

	makeResolverDependencies := func() (tnet.Identity, ipns.Name, ds.Datastore, routing.ValueStore) {
		ds := dssync.MutexWrap(ds.NewMapDatastore())
		id := tnet.RandIdentityOrFatal(t)
		r := offline.NewOfflineRouter(ds, record.NamespacedValidator{
			"ipns": ipns.Validator{}, // No need for KeyBook, as records created by NameSys include PublicKey for RSA.
			"pk":   record.PublicKeyValidator{},
		})

		return id, ipns.NameFromPeer(id.ID()), ds, r
	}

	t.Run("Publish and resolve", func(t *testing.T) {
		t.Parallel()

		id, name, ds, r := makeResolverDependencies()
		resolver := NewIPNSResolver(r)
		publisher := NewIPNSPublisher(r, ds)

		err := publisher.Publish(context.Background(), id.PrivateKey(), pathCat)
		require.NoError(t, err)

		res, err := resolver.Resolve(context.Background(), name.AsPath())
		require.NoError(t, err)
		require.Equal(t, pathCat, res.Path)
	})

	t.Run("Resolve does not return expired record", func(t *testing.T) {
		t.Parallel()

		id, name, ds, r := makeResolverDependencies()
		resolver := NewIPNSResolver(r)

		// Create a "bad" publisher that allows to publish expired records.
		publisher := NewIPNSPublisher(offline.NewOfflineRouter(ds, record.NamespacedValidator{
			"ipns": noFailValidator{},
			"pk":   record.PublicKeyValidator{},
		}), ds)

		// Publish expired.
		eol := time.Now().Add(time.Hour * -1)
		err := publisher.Publish(context.Background(), id.PrivateKey(), pathCat, PublishWithEOL(eol))
		require.NoError(t, err)

		// Expect to not be able to resolve.
		_, err = resolver.Resolve(context.Background(), name.AsPath())
		require.ErrorIs(t, err, ErrResolveFailed)
	})

	t.Run("Resolve prefers non-expired record", func(t *testing.T) {
		t.Parallel()

		id, name, ds, r := makeResolverDependencies()
		resolver := NewIPNSResolver(r)

		// Create a "bad" publisher that allows to publish expired records.
		publisher := NewIPNSPublisher(offline.NewOfflineRouter(ds, record.NamespacedValidator{
			"ipns": noFailValidator{},
			"pk":   record.PublicKeyValidator{},
		}), ds)

		// Publish expired.
		eol := time.Now().Add(time.Hour * -1)
		err := publisher.Publish(context.Background(), id.PrivateKey(), pathCat, PublishWithEOL(eol))
		require.NoError(t, err)

		// Publish new.
		err = publisher.Publish(context.Background(), id.PrivateKey(), pathDog)
		require.NoError(t, err)

		// Expect new.
		res, err := resolver.Resolve(context.Background(), name.AsPath())
		require.NoError(t, err)
		require.Equal(t, pathDog, res.Path)
	})

	t.Run("Resolve prefers newer record", func(t *testing.T) {
		t.Parallel()

		id, name, ds, r := makeResolverDependencies()
		resolver := NewIPNSResolver(r)
		publisher := NewIPNSPublisher(r, ds)

		// Publish one...
		err := publisher.Publish(context.Background(), id.PrivateKey(), pathCat, PublishWithEOL(time.Now().Add(time.Hour*2)))
		require.NoError(t, err)

		// Publish two...
		err = publisher.Publish(context.Background(), id.PrivateKey(), pathDog, PublishWithEOL(time.Now().Add(time.Hour*5)))
		require.NoError(t, err)

		// Should receive newer!
		res, err := resolver.Resolve(context.Background(), name.AsPath())
		require.NoError(t, err)
		require.Equal(t, pathDog, res.Path)
	})
}
