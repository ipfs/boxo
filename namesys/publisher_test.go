package namesys

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/ipfs/boxo/path"

	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	"github.com/ipfs/boxo/ipns"
	mockrouting "github.com/ipfs/boxo/routing/mock"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	testutil "github.com/libp2p/go-libp2p-testing/net"
	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type identity struct {
	testutil.PeerNetParams
}

func (p *identity) ID() peer.ID {
	return p.PeerNetParams.ID
}

func (p *identity) Address() ma.Multiaddr {
	return p.Addr
}

func (p *identity) PrivateKey() ci.PrivKey {
	return p.PrivKey
}

func (p *identity) PublicKey() ci.PubKey {
	return p.PubKey
}

func testNamekeyPublisher(t *testing.T, keyType int, expectedErr error, expectedExistence bool) {
	// Context
	ctx := context.Background()

	// Private key
	privKey, pubKey, err := ci.GenerateKeyPairWithReader(keyType, 2048, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	// ID
	id, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		t.Fatal(err)
	}

	// Value
	value, err := path.NewPath("/ipfs/bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	if err != nil {
		t.Fatal(err)
	}

	// Seqnum
	seqnum := uint64(0)

	// Eol
	eol := time.Now().Add(24 * time.Hour)

	// Routing value store
	p := testutil.PeerNetParams{
		ID:      id,
		PrivKey: privKey,
		PubKey:  pubKey,
		Addr:    testutil.ZeroLocalTCPAddress,
	}

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	serv := mockrouting.NewServer()
	r := serv.ClientWithDatastore(context.Background(), &identity{p}, dstore)

	rec, err := ipns.NewRecord(privKey, value, seqnum, eol, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = PutRecordToRouting(ctx, r, pubKey, rec)
	if err != nil {
		t.Fatal(err)
	}

	// Check for namekey existence in value store
	namekey := PkKeyForID(id)
	_, err = r.GetValue(ctx, namekey)
	if err != expectedErr {
		t.Fatal(err)
	}

	// Also check datastore for completeness
	key := dshelp.NewKeyFromBinary([]byte(namekey))
	exists, err := dstore.Has(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if exists != expectedExistence {
		t.Fatal("Unexpected key existence in datastore")
	}
}

func TestRSAPublisher(t *testing.T) {
	testNamekeyPublisher(t, ci.RSA, nil, true)
}

func TestEd22519Publisher(t *testing.T) {
	testNamekeyPublisher(t, ci.Ed25519, ds.ErrNotFound, false)
}

func TestAsyncDS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := mockrouting.NewServer().Client(testutil.RandIdentityOrFatal(t))
	ds := &checkSyncDS{
		Datastore: ds.NewMapDatastore(),
		syncKeys:  make(map[ds.Key]struct{}),
	}
	publisher := NewIpnsPublisher(rt, ds)

	ipnsFakeID := testutil.RandIdentityOrFatal(t)
	ipnsVal, err := path.NewPath("/ipns/foo.bar")
	if err != nil {
		t.Fatal(err)
	}

	if err := publisher.Publish(ctx, ipnsFakeID.PrivateKey(), ipnsVal); err != nil {
		t.Fatal(err)
	}

	ipnsKey := IpnsDsKey(ipnsFakeID.ID())

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
