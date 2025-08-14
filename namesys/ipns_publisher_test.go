package namesys

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	mockrouting "github.com/ipfs/boxo/routing/mock"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	testutil "github.com/libp2p/go-libp2p-testing/net"
	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestIPNSPublisher(t *testing.T) {
	t.Parallel()

	test := func(t *testing.T, keyType int, expectedErr error, expectedExistence bool) {
		ctx := context.Background()

		// Create test identity
		privKey, pubKey, err := ci.GenerateKeyPairWithReader(keyType, 2048, rand.Reader)
		require.NoError(t, err)

		pid, err := peer.IDFromPublicKey(pubKey)
		require.NoError(t, err)

		// Create IPNS Record
		value, err := path.NewPath("/ipfs/bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
		require.NoError(t, err)
		rec, err := ipns.NewRecord(privKey, value, 0, time.Now().Add(24*time.Hour), 0)
		require.NoError(t, err)

		// Routing value store
		dstore := dssync.MutexWrap(ds.NewMapDatastore())
		serv := mockrouting.NewServer()
		r := serv.ClientWithDatastore(context.Background(), testutil.NewIdentity(pid, testutil.ZeroLocalTCPAddress, privKey, pubKey), dstore)

		// Publish IPNS Record
		err = PublishIPNSRecord(ctx, r, pubKey, rec)
		require.NoError(t, err)

		// Check if IPNS Record is stored in value store
		_, err = r.GetValue(ctx, string(ipns.NameFromPeer(pid).RoutingKey()))
		require.NoError(t, err)

		key := dshelp.NewKeyFromBinary(ipns.NameFromPeer(pid).RoutingKey())
		exists, err := dstore.Has(ctx, key)
		require.NoError(t, err)
		require.True(t, exists)

		// Check for Public Key is stored in value store
		pkRoutingKey := PkRoutingKey(pid)
		_, err = r.GetValue(ctx, pkRoutingKey)
		require.ErrorIs(t, err, expectedErr)

		// Check if Public Key is in data store for completeness
		key = dshelp.NewKeyFromBinary([]byte(pkRoutingKey))
		exists, err = dstore.Has(ctx, key)
		require.NoError(t, err)
		require.Equal(t, expectedExistence, exists)
	}

	t.Run("RSA", func(t *testing.T) {
		t.Parallel()
		test(t, ci.RSA, nil, true)
	})

	t.Run("Ed22519", func(t *testing.T) {
		t.Parallel()
		test(t, ci.Ed25519, ds.ErrNotFound, false)
	})
}

func TestAsyncDS(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := mockrouting.NewServer().Client(testutil.RandIdentityOrFatal(t))
	ds := &checkSyncDS{
		Datastore: ds.NewMapDatastore(),
		syncKeys:  make(map[ds.Key]struct{}),
	}
	publisher := NewIPNSPublisher(rt, ds)

	ipnsFakeID := testutil.RandIdentityOrFatal(t)
	ipnsVal, err := path.NewPath("/ipns/foo.bar")
	require.NoError(t, err)

	err = publisher.Publish(ctx, ipnsFakeID.PrivateKey(), ipnsVal)
	require.NoError(t, err)

	ipnsKey := IpnsDsKey(ipns.NameFromPeer(ipnsFakeID.ID()))

	for k := range ds.syncKeys {
		if k.IsAncestorOf(ipnsKey) || k.Equal(ipnsKey) {
			return
		}
	}

	t.Fatal("ipns key not synced")
}

type checkSyncDS struct {
	ds.Datastore
	syncKeys map[ds.Key]struct{}
}

func (d *checkSyncDS) Sync(ctx context.Context, prefix ds.Key) error {
	d.syncKeys[prefix] = struct{}{}
	return d.Datastore.Sync(ctx, prefix)
}

func TestCustomSequenceValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Create test identity
	privKey, pubKey, err := ci.GenerateKeyPairWithReader(ci.Ed25519, 2048, rand.Reader)
	require.NoError(t, err)

	pid, err := peer.IDFromPublicKey(pubKey)
	require.NoError(t, err)

	// Setup publisher
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	serv := mockrouting.NewServer()
	r := serv.ClientWithDatastore(ctx, testutil.NewIdentity(pid, testutil.ZeroLocalTCPAddress, privKey, pubKey), dstore)
	publisher := NewIPNSPublisher(r, dstore)

	value1, err := path.NewPath("/ipfs/bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	require.NoError(t, err)

	value2, err := path.NewPath("/ipfs/bafkreihzrqy23ynilblgil62wy7gv22o4gklv2frcsgbwntnhptmzcq5tq")
	require.NoError(t, err)

	t.Run("custom sequence with no existing record", func(t *testing.T) {
		// Should work with any sequence number when no previous record exists
		err := publisher.Publish(ctx, privKey, value1, PublishWithSequence(42))
		require.NoError(t, err)

		// Verify the record was created with the custom sequence
		name := ipns.NameFromPeer(pid)
		rec, err := publisher.GetPublished(ctx, name, false)
		require.NoError(t, err)
		require.NotNil(t, rec)

		seq, err := rec.Sequence()
		require.NoError(t, err)
		require.Equal(t, uint64(42), seq)
	})

	t.Run("custom sequence greater than existing", func(t *testing.T) {
		// Should work with sequence greater than existing (42)
		err := publisher.Publish(ctx, privKey, value2, PublishWithSequence(50))
		require.NoError(t, err)

		// Verify the record was updated
		name := ipns.NameFromPeer(pid)
		rec, err := publisher.GetPublished(ctx, name, false)
		require.NoError(t, err)
		require.NotNil(t, rec)

		seq, err := rec.Sequence()
		require.NoError(t, err)
		require.Equal(t, uint64(50), seq)
	})

	t.Run("custom sequence equal to existing", func(t *testing.T) {
		// Should fail with sequence equal to existing (50)
		err := publisher.Publish(ctx, privKey, value1, PublishWithSequence(50))
		require.ErrorIs(t, err, ErrInvalidSequence)
	})

	t.Run("custom sequence less than existing", func(t *testing.T) {
		// Should fail with sequence less than existing (50)
		err := publisher.Publish(ctx, privKey, value2, PublishWithSequence(30))
		require.ErrorIs(t, err, ErrInvalidSequence)
	})

	t.Run("no custom sequence with existing record", func(t *testing.T) {
		// Should work normally, incrementing from 50 to 51
		err := publisher.Publish(ctx, privKey, value1) // Different value to trigger increment
		require.NoError(t, err)

		// Verify sequence was incremented
		name := ipns.NameFromPeer(pid)
		rec, err := publisher.GetPublished(ctx, name, false)
		require.NoError(t, err)
		require.NotNil(t, rec)

		seq, err := rec.Sequence()
		require.NoError(t, err)
		require.Equal(t, uint64(51), seq)
	})
}
