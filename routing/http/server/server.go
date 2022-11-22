package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/ipfs/go-cid"
	delegatedrouting "github.com/ipfs/go-delegated-routing"
	"github.com/ipfs/go-delegated-routing/internal/drjson"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	logging "github.com/ipfs/go-log/v2"
)

var logger = logging.Logger("service/server/delegatedrouting")

type ProvideRequest struct {
	Keys        []cid.Cid
	Timestamp   time.Time
	AdvisoryTTL time.Duration
	ID          peer.ID
	Addrs       []multiaddr.Multiaddr
}

type ContentRouter interface {
	FindProviders(ctx context.Context, key cid.Cid) ([]delegatedrouting.Provider, error)
	Provide(ctx context.Context, req ProvideRequest) (time.Duration, error)
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
	r.HandleFunc("/v1/providers", server.provide).Methods("POST")
	r.HandleFunc("/v1/providers/{cid}", server.findProviders).Methods("GET")

	return r
}

type server struct {
	svc    ContentRouter
	router *mux.Router
}

func (s *server) provide(w http.ResponseWriter, httpReq *http.Request) {
	req := delegatedrouting.WriteProvidersRequest{}
	err := json.NewDecoder(httpReq.Body).Decode(&req)
	if err != nil {
		writeErr(w, "Provide", http.StatusBadRequest, fmt.Errorf("invalid request: %w", err))
		return
	}

	resp := delegatedrouting.WriteProvidersResponse{}

	for i, prov := range req.Providers {
		switch v := prov.(type) {
		case *delegatedrouting.BitswapWriteProviderRequest:
			err := v.Verify()
			if err != nil {
				logErr("Provide", "signature verification failed", err)
				writeErr(w, "Provide", http.StatusForbidden, errors.New("signature verification failed"))
				return
			}

			keys := make([]cid.Cid, len(v.Keys))
			for i, k := range v.Keys {
				keys[i] = k.Cid

			}
			addrs := make([]multiaddr.Multiaddr, len(v.Addrs))
			for i, a := range v.Addrs {
				addrs[i] = a.Multiaddr
			}
			advisoryTTL, err := s.svc.Provide(httpReq.Context(), ProvideRequest{
				Keys:        keys,
				Timestamp:   v.Timestamp.Time,
				AdvisoryTTL: v.AdvisoryTTL.Duration,
				ID:          *v.ID,
				Addrs:       addrs,
			})
			if err != nil {
				writeErr(w, "Provide", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
				return
			}
			resp.Protocols = append(resp.Protocols, v.Protocol)
			resp.ProvideResults = append(resp.ProvideResults, &delegatedrouting.BitswapWriteProviderResponse{AdvisoryTTL: advisoryTTL})
		case *delegatedrouting.UnknownProvider:
			resp.Protocols = append(resp.Protocols, v.Protocol)
			resp.ProvideResults = append(resp.ProvideResults, v)
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
	response := delegatedrouting.FindProvidersResponse{Providers: providers}
	writeResult(w, "FindProviders", response)
}

func writeResult(w http.ResponseWriter, method string, val any) {
	// keep the marshaling separate from the writing, so we can distinguish bugs (which surface as 500)
	// from transient network issues (which surface as transport errors)
	buf, err := drjson.MarshalJSON(val)
	if err != nil {
		writeErr(w, method, http.StatusInternalServerError, fmt.Errorf("marshaling response: %w", err))
		return
	}
	_, err = io.Copy(w, buf)
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
