package test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-delegated-routing/client"
	proto "github.com/ipfs/go-delegated-routing/gen/proto"
	"github.com/ipld/edelweiss/values"
	"github.com/multiformats/go-multihash"
)

func TestClientWithServerReturningUnknownValues(t *testing.T) {

	// start a server
	s := httptest.NewServer(proto.DelegatedRouting_AsyncHandler(testServiceWithUnknown{}))
	defer s.Close()

	// start a client
	q, err := proto.New_DelegatedRouting_Client(s.URL, proto.DelegatedRouting_Client_WithHTTPClient(s.Client()))
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.NewClient(q, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// verify no result arrive
	h, err := multihash.Sum([]byte("TEST"), multihash.SHA3, 4)
	if err != nil {
		t.Fatal(err)
	}

	infos, err := c.FindProviders(context.Background(), cid.NewCidV1(cid.Raw, h))
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 0 {
		t.Fatalf("expecting 0 result, got %d", len(infos))
	}
}

type testServiceWithUnknown struct{}

func (testServiceWithUnknown) FindProviders(ctx context.Context, req *proto.FindProvidersRequest) (<-chan *proto.DelegatedRouting_FindProviders_AsyncResult, error) {
	respCh := make(chan *proto.DelegatedRouting_FindProviders_AsyncResult)
	go func() {
		defer close(respCh)
		respCh <- &proto.DelegatedRouting_FindProviders_AsyncResult{
			Resp: &proto.FindProvidersResponse{
				Providers: proto.ProvidersList{
					proto.Provider{
						ProviderNode: proto.Node{
							DefaultKey:   "UnknownNode",
							DefaultValue: &values.Any{Value: values.String("UnknownNodeValue")},
						},
						ProviderProto: proto.TransferProtocolList{
							proto.TransferProtocol{
								DefaultKey:   "UnknownProtocol",
								DefaultValue: &values.Any{Value: values.String("UnknownProtocolValue")},
							},
						},
					},
				},
			},
		}
	}()
	return respCh, nil
}

func (testServiceWithUnknown) GetIPNS(ctx context.Context, req *proto.GetIPNSRequest) (<-chan *proto.DelegatedRouting_GetIPNS_AsyncResult, error) {
	return nil, fmt.Errorf("GetIPNS not supported by test service")
}

func (testServiceWithUnknown) PutIPNS(ctx context.Context, req *proto.PutIPNSRequest) (<-chan *proto.DelegatedRouting_PutIPNS_AsyncResult, error) {
	return nil, fmt.Errorf("PutIPNS not supported by test service")
}

func (testServiceWithUnknown) Provide(ctx context.Context, req *proto.ProvideRequest) (<-chan *proto.DelegatedRouting_Provide_AsyncResult, error) {
	return nil, fmt.Errorf("Provide not supported by test service")
}
