package contentrouter

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockClient struct{ mock.Mock }

func (m *mockClient) ProvideBitswap(ctx context.Context, keys []cid.Cid, ttl time.Duration) (time.Duration, error) {
	args := m.Called(ctx, keys, ttl)
	return args.Get(0).(time.Duration), args.Error(1)
}

func (m *mockClient) FindProviders(ctx context.Context, key cid.Cid) (iter.ResultIter[types.Record], error) {
	args := m.Called(ctx, key)
	return args.Get(0).(iter.ResultIter[types.Record]), args.Error(1)
}

func (m *mockClient) FindPeers(ctx context.Context, pid peer.ID) (iter.ResultIter[*types.PeerRecord], error) {
	args := m.Called(ctx, pid)
	return args.Get(0).(iter.ResultIter[*types.PeerRecord]), args.Error(1)
}

func (m *mockClient) Ready(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
}

func (m *mockClient) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	args := m.Called(ctx, name)
	return args.Get(0).(*ipns.Record), args.Error(1)
}

func (m *mockClient) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	args := m.Called(ctx, name, record)
	return args.Error(0)
}

func (m *mockClient) GetClosestPeers(ctx context.Context, peerID peer.ID) (iter.ResultIter[*types.PeerRecord], error) {
	args := m.Called(ctx, peerID)
	return args.Get(0).(iter.ResultIter[*types.PeerRecord]), args.Error(1)
}

func TestProvide(t *testing.T) {
	for _, c := range []struct {
		name     string
		announce bool

		expNotProvided bool
	}{
		{
			name:           "announce=false results in no client request",
			announce:       false,
			expNotProvided: true,
		},
		{
			name:     "announce=true results in a client req",
			announce: true,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			key := makeCID()
			client := &mockClient{}
			crc := NewContentRoutingClient(client)

			if !c.expNotProvided {
				client.On("ProvideBitswap", ctx, []cid.Cid{key}, ttl).Return(time.Minute, nil)
			}

			err := crc.Provide(ctx, key, c.announce)
			assert.NoError(t, err)

			if c.expNotProvided {
				client.AssertNumberOfCalls(t, "ProvideBitswap", 0)
			}
		})
	}
}

func TestProvideMany(t *testing.T) {
	cids := []cid.Cid{makeCID(), makeCID()}
	var mhs []multihash.Multihash
	for _, c := range cids {
		mhs = append(mhs, c.Hash())
	}
	ctx := context.Background()
	client := &mockClient{}
	crc := NewContentRoutingClient(client)

	client.On("ProvideBitswap", ctx, cids, ttl).Return(time.Minute, nil)

	err := crc.ProvideMany(ctx, mhs)
	require.NoError(t, err)
}

func makeCID() cid.Cid {
	buf := make([]byte, 63)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err)
	}
	mh, err := multihash.Encode(buf, multihash.SHA2_256)
	if err != nil {
		panic(err)
	}
	c := cid.NewCidV1(cid.Raw, mh)
	return c
}

func TestFindProvidersAsync(t *testing.T) {
	key := makeCID()
	ctx := context.Background()
	client := &mockClient{}
	crc := NewContentRoutingClient(client)

	p1 := peer.ID("peer1")
	p2 := peer.ID("peer2")
	p3 := peer.ID("peer3")
	p4 := peer.ID("peer4")
	ais := []types.Record{
		&types.PeerRecord{
			Schema:    types.SchemaPeer,
			ID:        &p1,
			Protocols: []string{"transport-bitswap"},
		},
		//nolint:staticcheck
		//lint:ignore SA1019 // ignore staticcheck
		&types.BitswapRecord{
			//lint:ignore SA1019 // ignore staticcheck
			Schema:   types.SchemaBitswap,
			ID:       &p2,
			Protocol: "transport-bitswap",
		},
		&types.PeerRecord{
			Schema:    types.SchemaPeer,
			ID:        &p3,
			Protocols: []string{"transport-bitswap"},
		},
		&types.PeerRecord{
			Schema:    types.SchemaPeer,
			ID:        &p4,
			Protocols: []string{"transport-horse"},
		},
		&types.UnknownRecord{
			Schema: "unknown",
		},
	}
	aisIter := iter.ToResultIter[types.Record](iter.FromSlice(ais))

	client.On("FindProviders", ctx, key).Return(aisIter, nil)

	aiChan := crc.FindProvidersAsync(ctx, key, 3)

	var actualAIs []peer.AddrInfo
	for ai := range aiChan {
		actualAIs = append(actualAIs, ai)
	}

	expected := []peer.AddrInfo{
		{ID: p1},
		{ID: p2},
		{ID: p3},
		{ID: p4},
	}

	require.Equal(t, expected, actualAIs)
}

func TestFindPeer(t *testing.T) {
	ctx := context.Background()
	client := &mockClient{}
	crc := NewContentRoutingClient(client)

	p1 := peer.ID("peer1")
	ais := []*types.PeerRecord{
		{
			Schema:    types.SchemaPeer,
			ID:        &p1,
			Addrs:     []types.Multiaddr{{Multiaddr: multiaddr.StringCast("/ip4/1.2.3.4/tcp/1234")}},
			Protocols: []string{"transport-bitswap"},
		},
	}
	aisIter := iter.ToResultIter[*types.PeerRecord](iter.FromSlice(ais))

	client.On("FindPeers", ctx, p1).Return(aisIter, nil)

	p, err := crc.FindPeer(ctx, p1)
	require.NoError(t, err)
	require.Equal(t, p.ID, p1)
}

func TestFindPeerNoAddresses(t *testing.T) {
	ctx := context.Background()
	client := &mockClient{}
	crc := NewContentRoutingClient(client)

	p1 := peer.ID("peer1")
	ais := []*types.PeerRecord{
		{
			Schema:    types.SchemaPeer,
			ID:        &p1,
			Protocols: []string{"transport-bitswap"},
		},
	}
	aisIter := iter.ToResultIter[*types.PeerRecord](iter.FromSlice(ais))

	client.On("FindPeers", ctx, p1).Return(aisIter, nil)

	_, err := crc.FindPeer(ctx, p1)
	require.ErrorIs(t, err, routing.ErrNotFound)
}

func TestFindPeerWrongPeer(t *testing.T) {
	ctx := context.Background()
	client := &mockClient{}
	crc := NewContentRoutingClient(client)

	p1 := peer.ID("peer1")
	p2 := peer.ID("peer2")
	ais := []*types.PeerRecord{
		{
			Schema: types.SchemaPeer,
			ID:     &p2,
		},
	}
	aisIter := iter.ToResultIter[*types.PeerRecord](iter.FromSlice(ais))

	client.On("FindPeers", ctx, p1).Return(aisIter, nil)

	_, err := crc.FindPeer(ctx, p1)
	require.ErrorIs(t, err, routing.ErrNotFound)
}

func TestFindPeerNoPeer(t *testing.T) {
	ctx := context.Background()
	client := &mockClient{}
	crc := NewContentRoutingClient(client)

	p1 := peer.ID("peer1")
	aisIter := iter.ToResultIter[*types.PeerRecord](iter.FromSlice([]*types.PeerRecord{}))

	client.On("FindPeers", ctx, p1).Return(aisIter, nil)

	_, err := crc.FindPeer(ctx, p1)
	require.ErrorIs(t, err, routing.ErrNotFound)
}

func TestGetClosestPeers(t *testing.T) {
	t.Run("returns a channel and can read all results", func(t *testing.T) {
		ctx := context.Background()
		client := &mockClient{}
		crc := NewContentRoutingClient(client)

		peerID := peer.ID("test-peer")

		// Mock response with two peer records
		peer1 := peer.ID("peer1")
		peer2 := peer.ID("peer2")
		addr1 := multiaddr.StringCast("/ip4/1.2.3.4/tcp/1234")
		addr2 := multiaddr.StringCast("/ip4/5.6.7.8/tcp/5678")
		addrs1 := []types.Multiaddr{{Multiaddr: addr1}}
		addrs2 := []types.Multiaddr{{Multiaddr: addr2}}
		peerRec1 := &types.PeerRecord{
			Schema:    types.SchemaPeer,
			ID:        &peer1,
			Addrs:     addrs1,
			Protocols: []string{"transport-bitswap"},
		}
		peerRec2 := &types.PeerRecord{
			Schema:    types.SchemaPeer,
			ID:        &peer2,
			Addrs:     addrs2,
			Protocols: []string{"transport-bitswap"},
		}

		peerIter := iter.ToResultIter[*types.PeerRecord](iter.FromSlice([]*types.PeerRecord{peerRec1, peerRec2}))

		client.On("GetClosestPeers", ctx, peerID).Return(peerIter, nil)

		infos, err := crc.GetClosestPeers(ctx, peerID)
		require.NoError(t, err)

		var actual []peer.AddrInfo
		for info := range infos {
			actual = append(actual, info)
		}

		expected := []peer.AddrInfo{
			{ID: peer1, Addrs: []multiaddr.Multiaddr{addr1}},
			{ID: peer2, Addrs: []multiaddr.Multiaddr{addr2}},
		}

		assert.Equal(t, expected, actual)
	})

	t.Run("returns no results if addrs is empty", func(t *testing.T) {
		ctx := context.Background()
		client := &mockClient{}
		crc := NewContentRoutingClient(client)

		peerID := peer.ID("test-peer")

		peer1 := peer.ID("peer1")
		peerRec1 := &types.PeerRecord{
			Schema:    types.SchemaPeer,
			ID:        &peer1,
			Protocols: []string{"transport-bitswap"},
			// no addresses
		}

		// Mock response with an empty iterator
		peerIter := iter.ToResultIter[*types.PeerRecord](iter.FromSlice([]*types.PeerRecord{peerRec1}))

		client.On("GetClosestPeers", ctx, peerID).Return(peerIter, nil)

		infos, err := crc.GetClosestPeers(ctx, peerID)
		require.NoError(t, err)

		var actual []peer.AddrInfo
		for info := range infos {
			actual = append(actual, info)
		}

		assert.Empty(t, actual)
	})

	t.Run("returns an error if call errors", func(t *testing.T) {
		ctx := context.Background()
		client := &mockClient{}
		crc := NewContentRoutingClient(client)

		peerID := peer.ID("test-peer")

		// Mock error response
		peerIter := iter.ToResultIter[*types.PeerRecord](iter.FromSlice([]*types.PeerRecord{}))
		client.On("GetClosestPeers", ctx, peerID).Return(peerIter, assert.AnError)

		infos, err := crc.GetClosestPeers(ctx, peerID)
		require.ErrorIs(t, err, assert.AnError)
		assert.Nil(t, infos)
	})
}

func makeName(t *testing.T) (crypto.PrivKey, ipns.Name) {
	sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	pid, err := peer.IDFromPrivateKey(sk)
	require.NoError(t, err)

	return sk, ipns.NameFromPeer(pid)
}

func makeIPNSRecord(t *testing.T, sk crypto.PrivKey, opts ...ipns.Option) (*ipns.Record, []byte) {
	cid, err := cid.Decode("bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	require.NoError(t, err)

	path := path.FromCid(cid)
	eol := time.Now().Add(time.Hour * 48)
	ttl := time.Second * 20

	record, err := ipns.NewRecord(sk, path, 1, eol, ttl, opts...)
	require.NoError(t, err)

	rawRecord, err := ipns.MarshalRecord(record)
	require.NoError(t, err)

	return record, rawRecord
}

func TestGetValue(t *testing.T) {
	ctx := context.Background()
	client := &mockClient{}
	crc := NewContentRoutingClient(client)

	t.Run("Fail On Unsupported Key", func(t *testing.T) {
		v, err := crc.GetValue(ctx, "/something/unsupported")
		require.Nil(t, v)
		require.ErrorIs(t, err, routing.ErrNotSupported)
	})

	t.Run("Fail On Invalid IPNS Name", func(t *testing.T) {
		v, err := crc.GetValue(ctx, "/ipns/invalid")
		require.Nil(t, v)
		require.Error(t, err)
	})

	t.Run("Succeeds On Valid IPNS Name", func(t *testing.T) {
		sk, name := makeName(t)
		rec, rawRec := makeIPNSRecord(t, sk)
		client.On("GetIPNS", ctx, name).Return(rec, nil)
		v, err := crc.GetValue(ctx, string(name.RoutingKey()))
		require.NoError(t, err)
		require.Equal(t, rawRec, v)
	})
}

func TestPutValue(t *testing.T) {
	ctx := context.Background()
	client := &mockClient{}
	crc := NewContentRoutingClient(client)

	sk, name := makeName(t)
	_, rawRec := makeIPNSRecord(t, sk)

	t.Run("Fail On Unsupported Key", func(t *testing.T) {
		err := crc.PutValue(ctx, "/something/unsupported", rawRec)
		require.ErrorIs(t, err, routing.ErrNotSupported)
	})

	t.Run("Fail On Invalid IPNS Name", func(t *testing.T) {
		err := crc.PutValue(ctx, "/ipns/invalid", rawRec)
		require.Error(t, err)
	})

	t.Run("Fail On Invalid IPNS Record", func(t *testing.T) {
		err := crc.PutValue(ctx, string(name.RoutingKey()), []byte("gibberish"))
		require.Error(t, err)
	})

	t.Run("Succeeds On Valid IPNS Name & Record", func(t *testing.T) {
		client.On("PutIPNS", ctx, name, mock.Anything).Return(nil)
		err := crc.PutValue(ctx, string(name.RoutingKey()), rawRec)
		require.NoError(t, err)
	})
}
