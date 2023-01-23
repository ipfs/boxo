package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/routing/http/internal/drjson"
	"github.com/ipfs/go-libipfs/routing/http/types"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	logging "github.com/ipfs/go-log/v2"
)

var logger = logging.Logger("service/server/delegatedrouting")

const ProvidePath = "/routing/v1/providers/"
const FindProvidersPath = "/routing/v1/providers/{cid}"

type ContentRouter interface {
	FindProviders(ctx context.Context, key cid.Cid) ([]types.ProviderResponse, error)
	ProvideBitswap(ctx context.Context, req *BitswapWriteProvideRequest) (time.Duration, error)
	Provide(ctx context.Context, req *WriteProvideRequest) (types.ProviderResponse, error)
}

type BitswapWriteProvideRequest struct {
	Keys        []cid.Cid
	Timestamp   time.Time
	AdvisoryTTL time.Duration
	ID          peer.ID
	Addrs       []multiaddr.Multiaddr
}

type WriteProvideRequest struct {
	Protocol string
	Schema   string
	Bytes    []byte
}

type serverOption func(s *server)

func Handler(svc ContentRouter, opts ...serverOption) http.Handler {
	server := &server{
		svc: svc,
	}

	for _, opt := range opts {
		opt(server)
	}

	r := mux.NewRouter()
	r.HandleFunc(ProvidePath, server.provide).Methods(http.MethodPut)
	r.HandleFunc(FindProvidersPath, server.findProviders).Methods(http.MethodGet)

	return r
}

type server struct {
	svc ContentRouter
}

func (s *server) provide(w http.ResponseWriter, httpReq *http.Request) {
	req := types.WriteProvidersRequest{}
	err := json.NewDecoder(httpReq.Body).Decode(&req)
	_ = httpReq.Body.Close()
	if err != nil {
		writeErr(w, "Provide", http.StatusBadRequest, fmt.Errorf("invalid request: %w", err))
		return
	}

	resp := types.WriteProvidersResponse{}

	for i, prov := range req.Providers {
		switch v := prov.(type) {
		case *types.WriteBitswapProviderRecord:
			err := v.Verify()
			if err != nil {
				logErr("Provide", "signature verification failed", err)
				writeErr(w, "Provide", http.StatusForbidden, errors.New("signature verification failed"))
				return
			}

			keys := make([]cid.Cid, len(v.Payload.Keys))
			for i, k := range v.Payload.Keys {
				keys[i] = k.Cid

			}
			addrs := make([]multiaddr.Multiaddr, len(v.Payload.Addrs))
			for i, a := range v.Payload.Addrs {
				addrs[i] = a.Multiaddr
			}
			advisoryTTL, err := s.svc.ProvideBitswap(httpReq.Context(), &BitswapWriteProvideRequest{
				Keys:        keys,
				Timestamp:   v.Payload.Timestamp.Time,
				AdvisoryTTL: v.Payload.AdvisoryTTL.Duration,
				ID:          *v.Payload.ID,
				Addrs:       addrs,
			})
			if err != nil {
				writeErr(w, "Provide", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
				return
			}
			resp.ProvideResults = append(resp.ProvideResults,
				&types.WriteBitswapProviderRecordResponse{
					Protocol:    v.Protocol,
					Schema:      v.Schema,
					AdvisoryTTL: &types.Duration{Duration: advisoryTTL},
				},
			)
		case *types.UnknownProviderRecord:
			provResp, err := s.svc.Provide(httpReq.Context(), &WriteProvideRequest{
				Protocol: v.Protocol,
				Schema:   v.Schema,
				Bytes:    v.Bytes,
			})
			if err != nil {
				writeErr(w, "Provide", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
				return
			}
			resp.ProvideResults = append(resp.ProvideResults, provResp)
		default:
			writeErr(w, "Provide", http.StatusBadRequest, fmt.Errorf("provider record %d does not contain a protocol", i))
			return
		}
	}
	writeResult(w, "Provide", resp)
}

func (s *server) findProviders(w http.ResponseWriter, httpReq *http.Request) {
	vars := mux.Vars(httpReq)
	cidStr := vars["cid"]
	cid, err := cid.Decode(cidStr)
	if err != nil {
		writeErr(w, "FindProviders", http.StatusBadRequest, fmt.Errorf("unable to parse CID: %w", err))
		return
	}
	providers, err := s.svc.FindProviders(httpReq.Context(), cid)
	if err != nil {
		writeErr(w, "FindProviders", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
		return
	}
	response := types.ReadProvidersResponse{Providers: providers}
	writeResult(w, "FindProviders", response)
}

func writeResult(w http.ResponseWriter, method string, val any) {
	w.Header().Add("Content-Type", "application/json")

	// keep the marshaling separate from the writing, so we can distinguish bugs (which surface as 500)
	// from transient network issues (which surface as transport errors)
	b, err := drjson.MarshalJSONBytes(val)
	if err != nil {
		writeErr(w, method, http.StatusInternalServerError, fmt.Errorf("marshaling response: %w", err))
		return
	}

	_, err = io.Copy(w, bytes.NewBuffer(b))
	if err != nil {
		logErr("Provide", "writing response body", err)
	}
}

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
	logger.Infow(msg, "Method", method, "Error", err)
}
