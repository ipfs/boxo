package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-delegated-routing/client"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
)

var logger = logging.Logger("service/server/delegatedrouting")

type ContentRouter interface {
	FindProviders(ctx context.Context, key cid.Cid) ([]peer.AddrInfo, error)
	Provide(ctx context.Context, req *client.ProvideRequest) (time.Duration, error)
	Ready() bool
}

//	func DelegatedRoutingAsyncHandler(svc DelegatedRoutingService) http.HandlerFunc {
//		drs := &delegatedRoutingServer{svc}
//		return proto.DelegatedRouting_AsyncHandler(drs)
//	}
func Handler(svc ContentRouter) http.Handler {
	server := &server{
		svc: svc,
	}

	r := mux.NewRouter()
	// r.HandleFunc("/ipns/{id}", server.getIPNS).Methods("GET")
	// r.HandleFunc("/ipns", server.putIPNS).Methods("PUT")
	r.HandleFunc("/providers", server.provide).Methods("POST")
	r.HandleFunc("/providers/{cid}", server.findProviders).Methods("GET")

	return r
}

type server struct {
	svc    ContentRouter
	router *mux.Router
}

type provideRequest struct {
	
}

func (s *server) provide(w http.ResponseWriter, req *http.Request) {
	json.NewDecoder(req.Body).Decode(v any)
	recordBytes, err := s.svc.GetIPNS(req.Context(), []byte(id))
	_, err = io.Copy(w, bytes.NewBuffer(recordBytes))
	if err != nil {
		logErr("GetIPNS", "error writing response bytes", err)
	}
}
func (s *server) findProviders(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	cidStr := vars["cid"]
	cid, err := cid.Decode(cidStr)
	if err != nil {
		writeErr(w, "FindProviders", http.StatusBadRequest, fmt.Errorf("unable to parse CID: %w", err))
	}
}

// func (s *server) getIPNS(w http.ResponseWriter, req *http.Request) {
// 	vars := mux.Vars(req)
// 	idStr := vars["id"]
// 	id, err := peer.IDFromString(idStr)
// 	if err != nil {
// 		writeErr(w, "GetIPNS", http.StatusBadRequest, fmt.Errorf("unable to parse ID: %w", err))
// 		return
// 	}
// 	recordBytes, err := s.svc.GetIPNS(req.Context(), []byte(id))
// 	_, err = io.Copy(w, bytes.NewBuffer(recordBytes))
// 	if err != nil {
// 		logErr("GetIPNS", "error writing response bytes", err)
// 	}
// }

//	{
//	    "Records": [
//	        {
//	            "ID": "multibase bytes",
//	            "Record": "multibase bytes"
//	        }
//	    ]
//	}
// type putIPNSRequest struct {
// 	Records []putIPNSRequestRecord
// }

// type putIPNSRequestRecord struct {
// 	ID     string
// 	Record string
// }

// func (s *server) putIPNS(w http.ResponseWriter, httpReq *http.Request) {
// 	req := &putIPNSRequest{}
// 	err := json.NewDecoder(httpReq.Body).Decode(&req)
// 	if err != nil {
// 		writeErr(w, "PutIPNS", http.StatusBadRequest, fmt.Errorf("invalid request: %w", err))
// 		return
// 	}

// 	// validate
// 	if len(req.Records) > maxIPNSRecordsPerPut {
// 		writeErr(w, "PutIPNS", http.StatusBadRequest, fmt.Errorf("sent %d records but max is %d", len(req.Records), maxIPNSRecordsPerPut))
// 		return
// 	}
// 	type record struct {
// 		ID     peer.ID
// 		Record []byte
// 	}
// 	var records []record
// 	for i, rec := range req.Records {
// 		id, err := peer.IDFromString(rec.ID)
// 		if err != nil {
// 			writeErr(w, "PutIPNS", http.StatusBadRequest, fmt.Errorf("unable to parse ID of record %d: %w", i, err))
// 		}
// 		_, recordBytes, err := multibase.Decode(rec.Record)
// 		if err != nil {
// 			writeErr(w, "PutIPNS", http.StatusBadRequest, fmt.Errorf("unable to decode record bytes of record %d: %w", i, err))
// 		}
// 		records = append(records, record{
// 			ID:     id,
// 			Record: recordBytes,
// 		})
// 	}

// 	// execute
// 	ctx := httpReq.Context()
// 	for _, record := range records {
// 		if ctx.Err() != nil {
// 			writeErr(w, "PutIPNS", http.StatusInternalServerError, ctx.Err())
// 			return
// 		}
// 		err := s.svc.PutIPNS(ctx, []byte(record.ID), record.Record)
// 		if err != nil {

// 		}
// 	}
// }

func writeErr(w http.ResponseWriter, method string, statusCode int, cause error) {
	w.WriteHeader(statusCode)
	causeStr := cause.Error()
	if len(causeStr) > 1024 {
		causeStr = causeStr[:1024]
	}
	_, err := w.Write([]byte(causeStr))
	if err != nil {
		logErr(method, "error writing error cause", err)
		return
	}
}

func logErr(method, msg string, err error) {
	logger.Infof(msg, "Method", method, "Error", err)
}

// func (s *server) getIPNS(ctx context.Context, req *proto.GetIPNSRequest) ([]byte, error) {
// 	// rch := make(chan *proto.DelegatedRouting_GetIPNS_AsyncResult)
// 	// go func() {
// 	// 	defer close(rch)
// 	// 	id := req.ID
// 	// 	ch, err := drs.service.GetIPNS(ctx, id)
// 	// 	if err != nil {
// 	// 		logger.Errorf("get ipns function rejected request (%w)", err)
// 	// 		return
// 	// 	}

// 	// 	for {
// 	// 		select {
// 	// 		case <-ctx.Done():
// 	// 			return
// 	// 		case x, ok := <-ch:
// 	// 			if !ok {
// 	// 				return
// 	// 			}
// 	// 			var resp *proto.DelegatedRouting_GetIPNS_AsyncResult
// 	// 			if x.Err != nil {
// 	// 				logger.Infof("get ipns function returned error (%w)", x.Err)
// 	// 				resp = &proto.DelegatedRouting_GetIPNS_AsyncResult{Err: x.Err}
// 	// 			} else {
// 	// 				resp = &proto.DelegatedRouting_GetIPNS_AsyncResult{Resp: &proto.GetIPNSResponse{Record: x.Record}}
// 	// 			}

// 	// 			select {
// 	// 			case <-ctx.Done():
// 	// 				return
// 	// 			case rch <- resp:
// 	// 			}
// 	// 		}
// 	// 	}
// 	// }()
// 	// return rch, nil
// }

// func (s *server) PutIPNS(ctx context.Context, req *proto.PutIPNSRequest) error {
// 	// rch := make(chan *proto.DelegatedRouting_PutIPNS_AsyncResult)
// 	// go func() {
// 	// 	defer close(rch)
// 	// 	id, record := req.ID, req.Record
// 	// 	ch, err := drs.service.PutIPNS(ctx, id, record)
// 	// 	if err != nil {
// 	// 		logger.Errorf("put ipns function rejected request (%w)", err)
// 	// 		return
// 	// 	}

// 	// 	for {
// 	// 		select {
// 	// 		case <-ctx.Done():
// 	// 			return
// 	// 		case x, ok := <-ch:
// 	// 			if !ok {
// 	// 				return
// 	// 			}
// 	// 			var resp *proto.DelegatedRouting_PutIPNS_AsyncResult
// 	// 			if x.Err != nil {
// 	// 				logger.Infof("put ipns function returned error (%w)", x.Err)
// 	// 				resp = &proto.DelegatedRouting_PutIPNS_AsyncResult{Err: x.Err}
// 	// 			} else {
// 	// 				resp = &proto.DelegatedRouting_PutIPNS_AsyncResult{Resp: &proto.PutIPNSResponse{}}
// 	// 			}

// 	// 			select {
// 	// 			case <-ctx.Done():
// 	// 				return
// 	// 			case rch <- resp:
// 	// 			}
// 	// 		}
// 	// 	}
// 	// }()
// 	// return rch, nil
// }

// func (s *server) FindProviders(ctx context.Context, req *proto.FindProvidersRequest) ([]peer.AddrInfo, error) {
// 	// rch := make(chan *proto.DelegatedRouting_FindProviders_AsyncResult)
// 	// go func() {
// 	// 	defer close(rch)
// 	// 	pcids := parseCidsFromFindProvidersRequest(req)
// 	// 	for _, c := range pcids {
// 	// 		ch, err := drs.service.FindProviders(ctx, c)
// 	// 		if err != nil {
// 	// 			logger.Errorf("find providers function rejected request (%w)", err)
// 	// 			continue
// 	// 		}

// 	// 		for {
// 	// 			select {
// 	// 			case <-ctx.Done():
// 	// 				return
// 	// 			case x, ok := <-ch:
// 	// 				if !ok {
// 	// 					return
// 	// 				}
// 	// 				var resp *proto.DelegatedRouting_FindProviders_AsyncResult
// 	// 				if x.Err != nil {
// 	// 					logger.Infof("find providers function returned error (%w)", x.Err)
// 	// 					resp = &proto.DelegatedRouting_FindProviders_AsyncResult{Err: x.Err}
// 	// 				} else {
// 	// 					resp = buildFindProvidersResponse(c, x.AddrInfo)
// 	// 				}

// 	// 				select {
// 	// 				case <-ctx.Done():
// 	// 					return
// 	// 				case rch <- resp:
// 	// 				}
// 	// 			}
// 	// 		}
// 	// 	}
// 	// }()
// 	// return rch, nil
// }

// func (s *server) Provide(ctx context.Context, req *proto.ProvideRequest) (time.Duration, error) {
// 	// rch := make(chan *proto.DelegatedRouting_Provide_AsyncResult)
// 	// go func() {
// 	// 	defer close(rch)
// 	// 	pr, err := client.ParseProvideRequest(req)
// 	// 	if err != nil {
// 	// 		logger.Errorf("Provide function rejected request (%w)", err)
// 	// 		return
// 	// 	}
// 	// 	ch, err := drs.service.Provide(ctx, pr)
// 	// 	if err != nil {
// 	// 		logger.Errorf("Provide function rejected request (%w)", err)
// 	// 		return
// 	// 	}

// 	// 	for {
// 	// 		select {
// 	// 		case <-ctx.Done():
// 	// 			return
// 	// 		case resp, ok := <-ch:
// 	// 			if !ok {
// 	// 				return
// 	// 			}
// 	// 			var protoResp *proto.DelegatedRouting_Provide_AsyncResult
// 	// 			if resp.Err != nil {
// 	// 				logger.Infof("find providers function returned error (%w)", resp.Err)
// 	// 				protoResp = &proto.DelegatedRouting_Provide_AsyncResult{Err: resp.Err}
// 	// 			} else {
// 	// 				protoResp = &proto.DelegatedRouting_Provide_AsyncResult{Resp: &proto.ProvideResponse{AdvisoryTTL: values.Int(resp.AdvisoryTTL)}}
// 	// 			}

// 	// 			select {
// 	// 			case <-ctx.Done():
// 	// 				return
// 	// 			case rch <- protoResp:
// 	// 			}
// 	// 		}
// 	// 	}
// 	// }()
// 	// return rch, nil
// }

// // func parseCidsFromFindProvidersRequest(req *proto.FindProvidersRequest) []cid.Cid {
// // 	return []cid.Cid{cid.Cid(req.Key)}
// // }

// // func buildFindProvidersResponse(key cid.Cid, addrInfo []peer.AddrInfo) *proto.DelegatedRouting_FindProviders_AsyncResult {
// // 	provs := make(proto.ProvidersList, len(addrInfo))
// // 	bitswapProto := proto.TransferProtocol{Bitswap: &proto.BitswapProtocol{}}
// // 	for i, addrInfo := range addrInfo {
// // 		provs[i] = proto.Provider{
// // 			ProviderNode:  proto.Node{Peer: buildPeerFromAddrInfo(addrInfo)},
// // 			ProviderProto: proto.TransferProtocolList{bitswapProto},
// // 		}
// // 	}
// // 	return &proto.DelegatedRouting_FindProviders_AsyncResult{
// // 		Resp: &proto.FindProvidersResponse{Providers: provs},
// // 	}
// // }

// // func buildPeerFromAddrInfo(addrInfo peer.AddrInfo) *proto.Peer {
// // 	pm := make([]values.Bytes, len(addrInfo.Addrs))
// // 	for i, addr := range addrInfo.Addrs {
// // 		pm[i] = addr.Bytes()
// // 	}
// // 	return &proto.Peer{
// // 		ID:             []byte(addrInfo.ID),
// // 		Multiaddresses: pm,
// // 	}
// // }
