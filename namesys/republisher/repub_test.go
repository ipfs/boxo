package republisher_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jbenet/goprocess"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	host "github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"
	routing "github.com/libp2p/go-libp2p/core/routing"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	keystore "github.com/ipfs/boxo/keystore"
	"github.com/ipfs/boxo/namesys"
	. "github.com/ipfs/boxo/namesys/republisher"
)

type mockNode struct {
	h        host.Host
	id       peer.ID
	privKey  ic.PrivKey
	store    ds.Batching
	dht      *dht.IpfsDHT
	keystore keystore.Keystore
}

func getMockNode(t *testing.T, ctx context.Context) *mockNode {
	t.Helper()

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	var idht *dht.IpfsDHT
	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			rt, err := dht.New(ctx, h, dht.Mode(dht.ModeServer))
			idht = rt
			return rt, err
		}),
	)
	require.NoError(t, err)

	return &mockNode{
		h:        h,
		id:       h.ID(),
		privKey:  h.Peerstore().PrivKey(h.ID()),
		store:    dstore,
		dht:      idht,
		keystore: keystore.NewMemKeystore(),
	}
}

func TestRepublish(t *testing.T) {
	// set cache life to zero for testing low-period repubs

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var nsystems []namesys.NameSystem
	var nodes []*mockNode
	for i := 0; i < 10; i++ {
		n := getMockNode(t, ctx)
		ns, err := namesys.NewNameSystem(n.dht, namesys.WithDatastore(n.store))
		require.NoError(t, err)

		nsystems = append(nsystems, ns)
		nodes = append(nodes, n)
	}

	pinfo := host.InfoFromHost(nodes[0].h)

	for _, n := range nodes[1:] {
		err := n.h.Connect(ctx, *pinfo)
		require.NoError(t, err)
	}

	// have one node publish a record that is valid for 1 second
	publisher := nodes[3]

	p, err := path.NewPath("/ipfs/QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn") // does not need to be valid
	require.NoError(t, err)

	rp := namesys.NewIPNSPublisher(publisher.dht, publisher.store)
	name := ipns.NameFromPeer(publisher.id).AsPath()

	// Retry in case the record expires before we can fetch it. This can
	// happen when running the test on a slow machine.
	var expiration time.Time
	timeout := time.Second
	for {
		expiration = time.Now().Add(time.Second)
		err := rp.Publish(ctx, publisher.privKey, p, namesys.PublishWithEOL(expiration))
		require.NoError(t, err)

		err = verifyResolution(nsystems, name, p)
		if err == nil {
			break
		}

		if time.Now().After(expiration) {
			timeout *= 2
			continue
		}
		t.Fatal(err)
	}

	// Now wait a second, the records will be invalid and we should fail to resolve
	time.Sleep(timeout)
	err = verifyResolutionFails(nsystems, name)
	require.NoError(t, err)

	// The republishers that are contained within the nodes have their timeout set
	// to 12 hours. Instead of trying to tweak those, we're just going to pretend
	// they don't exist and make our own.
	repub := NewRepublisher(rp, publisher.store, publisher.privKey, publisher.keystore)
	repub.Interval = time.Second
	repub.RecordLifetime = time.Second * 5

	proc := goprocess.Go(repub.Run)
	defer proc.Close()

	// now wait a couple seconds for it to fire
	time.Sleep(time.Second * 2)

	// we should be able to resolve them now
	err = verifyResolution(nsystems, name, p)
	require.NoError(t, err)
}

func TestLongEOLRepublish(t *testing.T) {
	// set cache life to zero for testing low-period repubs

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var nsystems []namesys.NameSystem
	var nodes []*mockNode
	for i := 0; i < 10; i++ {
		n := getMockNode(t, ctx)
		ns, err := namesys.NewNameSystem(n.dht, namesys.WithDatastore(n.store))
		require.NoError(t, err)

		nsystems = append(nsystems, ns)
		nodes = append(nodes, n)
	}

	pinfo := host.InfoFromHost(nodes[0].h)

	for _, n := range nodes[1:] {
		err := n.h.Connect(ctx, *pinfo)
		require.NoError(t, err)
	}

	// have one node publish a record that is valid for 1 second
	publisher := nodes[3]
	p, err := path.NewPath("/ipfs/QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn")
	require.NoError(t, err)

	rp := namesys.NewIPNSPublisher(publisher.dht, publisher.store)
	name := ipns.NameFromPeer(publisher.id).AsPath()

	expiration := time.Now().Add(time.Hour)
	err = rp.Publish(ctx, publisher.privKey, p, namesys.PublishWithEOL(expiration))
	require.NoError(t, err)

	err = verifyResolution(nsystems, name, p)
	require.NoError(t, err)

	// The republishers that are contained within the nodes have their timeout set
	// to 12 hours. Instead of trying to tweak those, we're just going to pretend
	// they don't exist and make our own.
	repub := NewRepublisher(rp, publisher.store, publisher.privKey, publisher.keystore)
	repub.Interval = time.Millisecond * 500
	repub.RecordLifetime = time.Second

	proc := goprocess.Go(repub.Run)
	defer proc.Close()

	// now wait a couple seconds for it to fire a few times
	time.Sleep(time.Second * 2)

	err = verifyResolution(nsystems, name, p)
	require.NoError(t, err)

	rec, err := getLastIPNSRecord(ctx, publisher.store, ipns.NameFromPeer(publisher.h.ID()))
	require.NoError(t, err)

	finalEol, err := rec.Validity()
	require.NoError(t, err)
	require.Equal(t, expiration.UTC(), finalEol.UTC())
}

func getLastIPNSRecord(ctx context.Context, dstore ds.Datastore, name ipns.Name) (*ipns.Record, error) {
	// Look for it locally only
	val, err := dstore.Get(ctx, namesys.IpnsDsKey(name))
	if err != nil {
		return nil, err
	}

	return ipns.UnmarshalRecord(val)
}

func verifyResolution(nsystems []namesys.NameSystem, key path.Path, exp path.Path) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range nsystems {
		res, err := n.Resolve(ctx, key)
		if err != nil {
			return err
		}

		if res.Path.String() != exp.String() {
			return errors.New("resolved wrong record")
		}
	}
	return nil
}

func verifyResolutionFails(nsystems []namesys.NameSystem, key path.Path) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range nsystems {
		_, err := n.Resolve(ctx, key)
		if err == nil {
			return errors.New("expected resolution to fail")
		}
	}
	return nil
}
