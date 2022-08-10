package server

import (
	"context"
	"net/http"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-delegated-routing/client"
	proto "github.com/ipfs/go-delegated-routing/gen/proto"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/edelweiss/values"
	"github.com/libp2p/go-libp2p-core/peer"
)

var logger = logging.Logger("service/server/delegatedrouting")

type DelegatedRoutingService interface {
	FindProviders(ctx context.Context, key cid.Cid) (<-chan client.FindProvidersAsyncResult, error)
	GetIPNS(ctx context.Context, id []byte) (<-chan client.GetIPNSAsyncResult, error)
	PutIPNS(ctx context.Context, id []byte, record []byte) (<-chan client.PutIPNSAsyncResult, error)
	Provide(ctx context.Context, req *client.ProvideRequest) (<-chan client.ProvideAsyncResult, error)
}

func DelegatedRoutingAsyncHandler(svc DelegatedRoutingService) http.HandlerFunc {
	drs := &delegatedRoutingServer{svc}
	return proto.DelegatedRouting_AsyncHandler(drs)
}

type delegatedRoutingServer struct {
	service DelegatedRoutingService
}

func (drs *delegatedRoutingServer) GetIPNS(ctx context.Context, req *proto.GetIPNSRequest) (<-chan *proto.DelegatedRouting_GetIPNS_AsyncResult, error) {
	rch := make(chan *proto.DelegatedRouting_GetIPNS_AsyncResult)
	go func() {
		defer close(rch)
		id := req.ID
		ch, err := drs.service.GetIPNS(ctx, id)
		if err != nil {
			logger.Errorf("get ipns function rejected request (%w)", err)
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case x, ok := <-ch:
				if !ok {
					return
				}
				var resp *proto.DelegatedRouting_GetIPNS_AsyncResult
				if x.Err != nil {
					logger.Infof("get ipns function returned error (%w)", x.Err)
					resp = &proto.DelegatedRouting_GetIPNS_AsyncResult{Err: x.Err}
				} else {
					resp = &proto.DelegatedRouting_GetIPNS_AsyncResult{Resp: &proto.GetIPNSResponse{Record: x.Record}}
				}

				select {
				case <-ctx.Done():
					return
				case rch <- resp:
				}
			}
		}
	}()
	return rch, nil
}

func (drs *delegatedRoutingServer) PutIPNS(ctx context.Context, req *proto.PutIPNSRequest) (<-chan *proto.DelegatedRouting_PutIPNS_AsyncResult, error) {
	rch := make(chan *proto.DelegatedRouting_PutIPNS_AsyncResult)
	go func() {
		defer close(rch)
		id, record := req.ID, req.Record
		ch, err := drs.service.PutIPNS(ctx, id, record)
		if err != nil {
			logger.Errorf("put ipns function rejected request (%w)", err)
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case x, ok := <-ch:
				if !ok {
					return
				}
				var resp *proto.DelegatedRouting_PutIPNS_AsyncResult
				if x.Err != nil {
					logger.Infof("put ipns function returned error (%w)", x.Err)
					resp = &proto.DelegatedRouting_PutIPNS_AsyncResult{Err: x.Err}
				} else {
					resp = &proto.DelegatedRouting_PutIPNS_AsyncResult{Resp: &proto.PutIPNSResponse{}}
				}

				select {
				case <-ctx.Done():
					return
				case rch <- resp:
				}
			}
		}
	}()
	return rch, nil
}

func (drs *delegatedRoutingServer) FindProviders(ctx context.Context, req *proto.FindProvidersRequest) (<-chan *proto.DelegatedRouting_FindProviders_AsyncResult, error) {
	rch := make(chan *proto.DelegatedRouting_FindProviders_AsyncResult)
	go func() {
		defer close(rch)
		pcids := parseCidsFromFindProvidersRequest(req)
		for _, c := range pcids {
			ch, err := drs.service.FindProviders(ctx, c)
			if err != nil {
				logger.Errorf("find providers function rejected request (%w)", err)
				continue
			}

			for {
				select {
				case <-ctx.Done():
					return
				case x, ok := <-ch:
					if !ok {
						return
					}
					var resp *proto.DelegatedRouting_FindProviders_AsyncResult
					if x.Err != nil {
						logger.Infof("find providers function returned error (%w)", x.Err)
						resp = &proto.DelegatedRouting_FindProviders_AsyncResult{Err: x.Err}
					} else {
						resp = buildFindProvidersResponse(c, x.AddrInfo)
					}

					select {
					case <-ctx.Done():
						return
					case rch <- resp:
					}
				}
			}
		}
	}()
	return rch, nil
}

func (drs *delegatedRoutingServer) Provide(ctx context.Context, req *proto.ProvideRequest) (<-chan *proto.DelegatedRouting_Provide_AsyncResult, error) {
	rch := make(chan *proto.DelegatedRouting_Provide_AsyncResult)
	go func() {
		defer close(rch)
		pr, err := client.ParseProvideRequest(req)
		if err != nil {
			logger.Errorf("Provide function rejected request (%w)", err)
			return
		}
		ch, err := drs.service.Provide(ctx, pr)
		if err != nil {
			logger.Errorf("Provide function rejected request (%w)", err)
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-ch:
				if !ok {
					return
				}
				var protoResp *proto.DelegatedRouting_Provide_AsyncResult
				if resp.Err != nil {
					logger.Infof("find providers function returned error (%w)", resp.Err)
					protoResp = &proto.DelegatedRouting_Provide_AsyncResult{Err: resp.Err}
				} else {
					protoResp = &proto.DelegatedRouting_Provide_AsyncResult{Resp: &proto.ProvideResponse{AdvisoryTTL: values.Int(resp.AdvisoryTTL)}}
				}

				select {
				case <-ctx.Done():
					return
				case rch <- protoResp:
				}
			}
		}
	}()
	return rch, nil
}

func parseCidsFromFindProvidersRequest(req *proto.FindProvidersRequest) []cid.Cid {
	return []cid.Cid{cid.Cid(req.Key)}
}

func buildFindProvidersResponse(key cid.Cid, addrInfo []peer.AddrInfo) *proto.DelegatedRouting_FindProviders_AsyncResult {
	provs := make(proto.ProvidersList, len(addrInfo))
	bitswapProto := proto.TransferProtocol{Bitswap: &proto.BitswapProtocol{}}
	for i, addrInfo := range addrInfo {
		provs[i] = proto.Provider{
			ProviderNode:  proto.Node{Peer: buildPeerFromAddrInfo(addrInfo)},
			ProviderProto: proto.TransferProtocolList{bitswapProto},
		}
	}
	return &proto.DelegatedRouting_FindProviders_AsyncResult{
		Resp: &proto.FindProvidersResponse{Providers: provs},
	}
}

func buildPeerFromAddrInfo(addrInfo peer.AddrInfo) *proto.Peer {
	pm := make([]values.Bytes, len(addrInfo.Addrs))
	for i, addr := range addrInfo.Addrs {
		pm[i] = addr.Bytes()
	}
	return &proto.Peer{
		ID:             []byte(addrInfo.ID),
		Multiaddresses: pm,
	}
}
