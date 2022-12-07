package contentrouter

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/routing/http/types"
	"github.com/libp2p/go-libp2p/core/peer"
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
func (m *mockClient) FindProviders(ctx context.Context, key cid.Cid) ([]types.ProviderResponse, error) {
	args := m.Called(ctx, key)
	return args.Get(0).([]types.ProviderResponse), args.Error(1)
}
func (m *mockClient) Ready(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
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

func TestFindProvidersAsync(t *testing.T) {
	key := makeCID()
	ctx := context.Background()
	client := &mockClient{}
	crc := NewContentRoutingClient(client)

	p1 := peer.ID("peer1")
	p2 := peer.ID("peer2")
	ais := []types.ProviderResponse{
		&types.ReadBitswapProviderRecord{
			Protocol: "transport-bitswap",
			Schema:   types.SchemaBitswap,
			ID:       &p1,
		},
		&types.ReadBitswapProviderRecord{
			Protocol: "transport-bitswap",
			Schema:   types.SchemaBitswap,
			ID:       &p2,
		},
		&types.UnknownProviderRecord{
			Protocol: "UNKNOWN",
		},
	}

	client.On("FindProviders", ctx, key).Return(ais, nil)

	aiChan := crc.FindProvidersAsync(ctx, key, 2)

	var actualAIs []peer.AddrInfo
	for ai := range aiChan {
		actualAIs = append(actualAIs, ai)
	}

	expected := []peer.AddrInfo{
		{ID: p1},
		{ID: p2},
	}

	require.Equal(t, expected, actualAIs)
}
