package contentrouter

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockClient struct{ mock.Mock }

func (m *mockClient) FindProviders(ctx context.Context, key cid.Cid) (iter.ResultIter[types.Record], error) {
	args := m.Called(ctx, key)
	return args.Get(0).(iter.ResultIter[types.Record]), args.Error(1)
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

func TestFindProvidersAsync(t *testing.T) {
	key := makeCID()
	ctx := context.Background()
	client := &mockClient{}
	crc := NewContentRoutingClient(client)

	p1 := peer.ID("peer1")
	p2 := peer.ID("peer2")
	ais := []types.Record{
		&types.PeerRecord{
			Schema:    types.SchemaPeer,
			ID:        &p1,
			Protocols: []string{"transport-bitswap"},
		},
		&types.PeerRecord{
			Schema:    types.SchemaPeer,
			ID:        &p2,
			Protocols: []string{"transport-bitswap"},
		},
		// &types.UnknownRecord{
		// 	Protocol: "UNKNOWN",
		// },
	}
	aisIter := iter.ToResultIter[types.Record](iter.FromSlice(ais))

	client.On("FindProviders", ctx, key).Return(aisIter, nil)

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
