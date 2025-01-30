package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/gorilla/mux"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/filters"
	"github.com/ipfs/boxo/routing/http/internal/drjson"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	jsontypes "github.com/ipfs/boxo/routing/http/types/json"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multibase"
	"github.com/prometheus/client_golang/prometheus"
	metrics "github.com/slok/go-http-metrics/metrics/prometheus"
	"github.com/slok/go-http-metrics/middleware"
	middlewarestd "github.com/slok/go-http-metrics/middleware/std"
)

const (
	mediaTypeJSON       = "application/json"
	mediaTypeNDJSON     = "application/x-ndjson"
	mediaTypeWildcard   = "*/*"
	mediaTypeIPNSRecord = "application/vnd.ipfs.ipns-record"

	DefaultRecordsLimit          = 20
	DefaultStreamingRecordsLimit = 0
	DefaultRoutingTimeout        = 30 * time.Second
)

var logger = logging.Logger("routing/http/server")

const (
	providePath       = "/routing/v1/providers/"
	findProvidersPath = "/routing/v1/providers/{cid}"
	findPeersPath     = "/routing/v1/peers/{peer-id}"
	GetIPNSPath       = "/routing/v1/ipns/{cid}"
)

type FindProvidersAsyncResponse struct {
	ProviderResponse types.Record
	Error            error
}

type ContentRouter interface {
	// FindProviders searches for peers who are able to provide the given [cid.Cid].
	// Limit indicates the maximum amount of results to return; 0 means unbounded.
	FindProviders(ctx context.Context, cid cid.Cid, limit int) (iter.ResultIter[types.Record], error)

	// Deprecated: protocol-agnostic provide is being worked on in [IPIP-378]:
	//
	// [IPIP-378]: https://github.com/ipfs/specs/pull/378
	ProvideBitswap(ctx context.Context, req *BitswapWriteProvideRequest) (time.Duration, error)

	// FindPeers searches for peers who have the provided [peer.ID].
	// Limit indicates the maximum amount of results to return; 0 means unbounded.
	FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error)

	// GetIPNS searches for an [ipns.Record] for the given [ipns.Name].
	GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error)

	// PutIPNS stores the provided [ipns.Record] for the given [ipns.Name].
	// It is guaranteed that the record matches the provided name.
	PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error
}

// Deprecated: protocol-agnostic provide is being worked on in [IPIP-378]:
//
// [IPIP-378]: https://github.com/ipfs/specs/pull/378
type BitswapWriteProvideRequest struct {
	Keys        []cid.Cid
	Timestamp   time.Time
	AdvisoryTTL time.Duration
	ID          peer.ID
	Addrs       []multiaddr.Multiaddr
}

// Deprecated: protocol-agnostic provide is being worked on in [IPIP-378]:
//
// [IPIP-378]: https://github.com/ipfs/specs/pull/378
type WriteProvideRequest struct {
	Protocol string
	Schema   string
	Bytes    []byte
}

type Option func(s *server)

// WithStreamingResultsDisabled disables ndjson responses, so that the server only supports JSON responses.
func WithStreamingResultsDisabled() Option {
	return func(s *server) {
		s.disableNDJSON = true
	}
}

// WithRecordsLimit sets a limit that will be passed to [ContentRouter.FindProviders]
// and [ContentRouter.FindPeers] for non-streaming requests (application/json).
// Default is [DefaultRecordsLimit].
func WithRecordsLimit(limit int) Option {
	return func(s *server) {
		s.recordsLimit = limit
	}
}

// WithStreamingRecordsLimit sets a limit that will be passed to [ContentRouter.FindProviders]
// and [ContentRouter.FindPeers] for streaming requests (application/x-ndjson).
// Default is [DefaultStreamingRecordsLimit].
func WithStreamingRecordsLimit(limit int) Option {
	return func(s *server) {
		s.streamingRecordsLimit = limit
	}
}

func WithPrometheusRegistry(reg prometheus.Registerer) Option {
	return func(s *server) {
		s.promRegistry = reg
	}
}

func WithRoutingTimeout(timeout time.Duration) Option {
	return func(s *server) {
		s.routingTimeout = timeout
	}
}

func Handler(svc ContentRouter, opts ...Option) http.Handler {
	server := &server{
		svc:                   svc,
		recordsLimit:          DefaultRecordsLimit,
		streamingRecordsLimit: DefaultStreamingRecordsLimit,
		routingTimeout:        DefaultRoutingTimeout,
	}

	for _, opt := range opts {
		opt(server)
	}

	if server.promRegistry == nil {
		server.promRegistry = prometheus.DefaultRegisterer
	}

	// Workaround due to https://github.com/slok/go-http-metrics
	// using egistry.MustRegister internally.
	// In production there will be only one handler, however we append counter
	// to ensure duplicate metric registration will not panic in parallel tests
	// when global prometheus.DefaultRegisterer is used.
	metricsPrefix := "delegated_routing_server"
	c := handlerCount.Add(1)
	if c > 1 {
		metricsPrefix = fmt.Sprintf("%s_%d", metricsPrefix, c)
	}

	// Create middleware with prometheus recorder
	mdlw := middleware.New(middleware.Config{
		Recorder: metrics.NewRecorder(metrics.Config{
			Registry:        server.promRegistry,
			Prefix:          metricsPrefix,
			SizeBuckets:     prometheus.ExponentialBuckets(100, 4, 8), // [100 400 1600 6400 25600 102400 409600 1.6384e+06]
			DurationBuckets: []float64{0.1, 0.5, 1, 2, 5, 8, 10, 20, 30},
		}),
	})

	r := mux.NewRouter()
	// Wrap each handler with the metrics middleware
	r.Handle(findProvidersPath, middlewarestd.Handler(findProvidersPath, mdlw, http.HandlerFunc(server.findProviders))).Methods(http.MethodGet)
	r.Handle(providePath, middlewarestd.Handler(providePath, mdlw, http.HandlerFunc(server.provide))).Methods(http.MethodPut)
	r.Handle(findPeersPath, middlewarestd.Handler(findPeersPath, mdlw, http.HandlerFunc(server.findPeers))).Methods(http.MethodGet)
	r.Handle(GetIPNSPath, middlewarestd.Handler(GetIPNSPath, mdlw, http.HandlerFunc(server.GetIPNS))).Methods(http.MethodGet)
	r.Handle(GetIPNSPath, middlewarestd.Handler(GetIPNSPath, mdlw, http.HandlerFunc(server.PutIPNS))).Methods(http.MethodPut)

	return r
}

var handlerCount atomic.Int32

type server struct {
	svc                   ContentRouter
	disableNDJSON         bool
	recordsLimit          int
	streamingRecordsLimit int
	promRegistry          prometheus.Registerer
	routingTimeout        time.Duration
}

func (s *server) detectResponseType(r *http.Request) (string, error) {
	var (
		supportsNDJSON bool
		supportsJSON   bool

		acceptHeaders = r.Header.Values("Accept")
	)

	if len(acceptHeaders) == 0 {
		return mediaTypeJSON, nil
	}

	for _, acceptHeader := range acceptHeaders {
		for _, accept := range strings.Split(acceptHeader, ",") {
			mediaType, _, err := mime.ParseMediaType(accept)
			if err != nil {
				return "", fmt.Errorf("unable to parse Accept header: %w", err)
			}

			switch mediaType {
			case mediaTypeJSON, mediaTypeWildcard:
				supportsJSON = true
			case mediaTypeNDJSON:
				supportsNDJSON = true
			}
		}
	}

	if supportsNDJSON && !s.disableNDJSON {
		return mediaTypeNDJSON, nil
	} else if supportsJSON {
		return mediaTypeJSON, nil
	} else {
		return "", errors.New("no supported content types")
	}
}

func (s *server) findProviders(w http.ResponseWriter, httpReq *http.Request) {
	vars := mux.Vars(httpReq)
	cidStr := vars["cid"]
	cid, err := cid.Decode(cidStr)
	if err != nil {
		writeErr(w, "FindProviders", http.StatusBadRequest, fmt.Errorf("unable to parse CID: %w", err))
		return
	}

	// Parse query parameters
	query := httpReq.URL.Query()
	filterAddrs := filters.ParseFilter(query.Get("filter-addrs"))
	filterProtocols := filters.ParseFilter(query.Get("filter-protocols"))

	mediaType, err := s.detectResponseType(httpReq)
	if err != nil {
		writeErr(w, "FindProviders", http.StatusBadRequest, err)
		return
	}

	var (
		handlerFunc  func(w http.ResponseWriter, provIter iter.ResultIter[types.Record], filterAddrs, filterProtocols []string)
		recordsLimit int
	)

	if mediaType == mediaTypeNDJSON {
		handlerFunc = s.findProvidersNDJSON
		recordsLimit = s.streamingRecordsLimit
	} else {
		handlerFunc = s.findProvidersJSON
		recordsLimit = s.recordsLimit
	}

	ctx, cancel := context.WithTimeout(httpReq.Context(), s.routingTimeout)
	defer cancel()

	provIter, err := s.svc.FindProviders(ctx, cid, recordsLimit)
	if err != nil {
		if errors.Is(err, routing.ErrNotFound) {
			// handlerFunc takes care of setting the 404 and necessary headers
			provIter = iter.FromSlice([]iter.Result[types.Record]{})
		} else {
			writeErr(w, "FindProviders", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
			return
		}
	}

	handlerFunc(w, provIter, filterAddrs, filterProtocols)
}

func (s *server) findProvidersJSON(w http.ResponseWriter, provIter iter.ResultIter[types.Record], filterAddrs, filterProtocols []string) {
	defer provIter.Close()

	filteredIter := filters.ApplyFiltersToIter(provIter, filterAddrs, filterProtocols)
	providers, err := iter.ReadAllResults(filteredIter)
	if err != nil {
		writeErr(w, "FindProviders", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
		return
	}

	writeJSONResult(w, "FindProviders", jsontypes.ProvidersResponse{
		Providers: providers,
	})
}

func (s *server) findProvidersNDJSON(w http.ResponseWriter, provIter iter.ResultIter[types.Record], filterAddrs, filterProtocols []string) {
	filteredIter := filters.ApplyFiltersToIter(provIter, filterAddrs, filterProtocols)

	writeResultsIterNDJSON(w, filteredIter)
}

func (s *server) findPeers(w http.ResponseWriter, r *http.Request) {
	pidStr := mux.Vars(r)["peer-id"]

	// While specification states that peer-id is expected to be in CIDv1 format, reality
	// is the clients will often learn legacy PeerID string from other sources,
	// and try to use it.
	// See https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation
	// We are liberal in inputs here, and uplift legacy PeerID to CID if necessary.
	// Rationale: it is better to fix this common mistake than to error and break peer routing.

	// Attempt to parse PeerID
	pid, err := peer.Decode(pidStr)
	if err != nil {
		// Retry by parsing PeerID as CID, then setting codec to libp2p-key
		// and turning that back to PeerID.
		// This is necessary to make sure legacy keys like:
		// - RSA QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N
		// - ED25519 12D3KooWD3eckifWpRn9wQpMG9R9hX3sD158z7EqHWmweQAJU5SA
		// are parsed correctly.
		pidAsCid, err2 := cid.Decode(pidStr)
		if err2 == nil {
			pidAsCid = cid.NewCidV1(cid.Libp2pKey, pidAsCid.Hash())
			pid, err = peer.FromCid(pidAsCid)
		}
	}

	if err != nil {
		writeErr(w, "FindPeers", http.StatusBadRequest, fmt.Errorf("unable to parse PeerID %q: %w", pidStr, err))
		return
	}

	query := r.URL.Query()
	filterAddrs := filters.ParseFilter(query.Get("filter-addrs"))
	filterProtocols := filters.ParseFilter(query.Get("filter-protocols"))

	mediaType, err := s.detectResponseType(r)
	if err != nil {
		writeErr(w, "FindPeers", http.StatusBadRequest, err)
		return
	}

	var (
		handlerFunc  func(w http.ResponseWriter, provIter iter.ResultIter[*types.PeerRecord], filterAddrs, filterProtocols []string)
		recordsLimit int
	)

	if mediaType == mediaTypeNDJSON {
		handlerFunc = s.findPeersNDJSON
		recordsLimit = s.streamingRecordsLimit
	} else {
		handlerFunc = s.findPeersJSON
		recordsLimit = s.recordsLimit
	}

	// Add timeout to the routing operation
	ctx, cancel := context.WithTimeout(r.Context(), s.routingTimeout)
	defer cancel()

	provIter, err := s.svc.FindPeers(ctx, pid, recordsLimit)
	if err != nil {
		if errors.Is(err, routing.ErrNotFound) {
			// handlerFunc takes care of setting the 404 and necessary headers
			provIter = iter.FromSlice([]iter.Result[*types.PeerRecord]{})
		} else {
			writeErr(w, "FindPeers", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
			return
		}
	}

	handlerFunc(w, provIter, filterAddrs, filterProtocols)
}

func (s *server) provide(w http.ResponseWriter, httpReq *http.Request) {
	//nolint:staticcheck
	//lint:ignore SA1019 // ignore staticcheck
	req := jsontypes.WriteProvidersRequest{}
	err := json.NewDecoder(httpReq.Body).Decode(&req)
	_ = httpReq.Body.Close()
	if err != nil {
		writeErr(w, "Provide", http.StatusBadRequest, fmt.Errorf("invalid request: %w", err))
		return
	}

	//nolint:staticcheck
	//lint:ignore SA1019 // ignore staticcheck
	resp := jsontypes.WriteProvidersResponse{}

	for i, prov := range req.Providers {
		switch v := prov.(type) {
		//nolint:staticcheck
		//lint:ignore SA1019 // ignore staticcheck
		case *types.WriteBitswapRecord:
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
				//lint:ignore SA1019 // ignore staticcheck
				&types.WriteBitswapRecordResponse{
					Protocol:    v.Protocol,
					Schema:      v.Schema,
					AdvisoryTTL: &types.Duration{Duration: advisoryTTL},
				},
			)
		default:
			writeErr(w, "Provide", http.StatusBadRequest, fmt.Errorf("provider record %d is not bitswap", i))
			return
		}
	}
	writeJSONResult(w, "Provide", resp)
}

func (s *server) findPeersJSON(w http.ResponseWriter, peersIter iter.ResultIter[*types.PeerRecord], filterAddrs, filterProtocols []string) {
	defer peersIter.Close()

	peersIter = filters.ApplyFiltersToPeerRecordIter(peersIter, filterAddrs, filterProtocols)

	peers, err := iter.ReadAllResults(peersIter)
	if err != nil {
		writeErr(w, "FindPeers", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
		return
	}

	writeJSONResult(w, "FindPeers", jsontypes.PeersResponse{
		Peers: peers,
	})
}

func (s *server) findPeersNDJSON(w http.ResponseWriter, peersIter iter.ResultIter[*types.PeerRecord], filterAddrs, filterProtocols []string) {
	// Convert PeerRecord to Record so that we can reuse the filtering logic from findProviders
	mappedIter := iter.Map(peersIter, func(v iter.Result[*types.PeerRecord]) iter.Result[types.Record] {
		if v.Err != nil || v.Val == nil {
			return iter.Result[types.Record]{Err: v.Err}
		}

		var record types.Record = v.Val
		return iter.Result[types.Record]{Val: record}
	})

	filteredIter := filters.ApplyFiltersToIter(mappedIter, filterAddrs, filterProtocols)
	writeResultsIterNDJSON(w, filteredIter)
}

func (s *server) GetIPNS(w http.ResponseWriter, r *http.Request) {
	acceptHdrValue := r.Header.Get("Accept")
	// When 'Accept' header is missing, default to 'application/vnd.ipfs.ipns-record'
	// (improved UX, similar to how we default to JSON response for /providers and /peers)
	if len(acceptHdrValue) == 0 || strings.Contains(acceptHdrValue, mediaTypeWildcard) {
		acceptHdrValue = mediaTypeIPNSRecord
	}
	if !strings.Contains(acceptHdrValue, mediaTypeIPNSRecord) {
		writeErr(w, "GetIPNS", http.StatusNotAcceptable, errors.New("content type in 'Accept' header is not supported, retry with 'application/vnd.ipfs.ipns-record'"))
		return
	}

	vars := mux.Vars(r)
	cidStr := vars["cid"]
	cid, err := cid.Decode(cidStr)
	if err != nil {
		writeErr(w, "GetIPNS", http.StatusBadRequest, fmt.Errorf("unable to parse CID: %w", err))
		return
	}

	name, err := ipns.NameFromCid(cid)
	if err != nil {
		writeErr(w, "GetIPNS", http.StatusBadRequest, fmt.Errorf("peer ID CID is not valid: %w", err))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.routingTimeout)
	defer cancel()

	record, err := s.svc.GetIPNS(ctx, name)
	if err != nil {
		if errors.Is(err, routing.ErrNotFound) {
			writeErr(w, "GetIPNS", http.StatusNotFound, fmt.Errorf("delegate error: %w", err))
			return
		} else {
			writeErr(w, "GetIPNS", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
			return
		}
	}

	rawRecord, err := ipns.MarshalRecord(record)
	if err != nil {
		writeErr(w, "GetIPNS", http.StatusInternalServerError, err)
		return
	}

	var remainingValidity int
	// Include 'Expires' header with time when signature expiration happens
	if validityType, err := record.ValidityType(); err == nil && validityType == ipns.ValidityEOL {
		if validity, err := record.Validity(); err == nil {
			w.Header().Set("Expires", validity.UTC().Format(http.TimeFormat))
			remainingValidity = int(time.Until(validity).Seconds())
		}
	} else {
		remainingValidity = int(ipns.DefaultRecordLifetime.Seconds())
	}
	if ttl, err := record.TTL(); err == nil {
		setCacheControl(w, int(ttl.Seconds()), remainingValidity)
	} else {
		setCacheControl(w, int(ipns.DefaultRecordTTL.Seconds()), remainingValidity)
	}
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))

	w.Header().Set("Etag", fmt.Sprintf(`"%x"`, xxhash.Sum64(rawRecord)))
	w.Header().Set("Content-Type", mediaTypeIPNSRecord)

	// Content-Disposition is not required, but improves UX by assigning a meaningful filename when opening URL in a web browser
	if filename, err := cid.StringOfBase(multibase.Base36); err == nil {
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s.ipns-record\"", filename))
	}
	w.Header().Add("Vary", "Accept")
	w.Write(rawRecord)
}

func (s *server) PutIPNS(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.Header.Get("Content-Type"), mediaTypeIPNSRecord) {
		writeErr(w, "PutIPNS", http.StatusNotAcceptable, errors.New("content type in 'Content-Type' header is missing or not supported"))
		return
	}

	vars := mux.Vars(r)
	cidStr := vars["cid"]
	cid, err := cid.Decode(cidStr)
	if err != nil {
		writeErr(w, "PutIPNS", http.StatusBadRequest, fmt.Errorf("unable to parse CID: %w", err))
		return
	}

	name, err := ipns.NameFromCid(cid)
	if err != nil {
		writeErr(w, "PutIPNS", http.StatusBadRequest, fmt.Errorf("peer ID CID is not valid: %w", err))
		return
	}

	// Limit the reader to the maximum record size.
	rawRecord, err := io.ReadAll(io.LimitReader(r.Body, int64(ipns.MaxRecordSize)))
	if err != nil {
		writeErr(w, "PutIPNS", http.StatusBadRequest, fmt.Errorf("provided record is too long: %w", err))
		return
	}

	record, err := ipns.UnmarshalRecord(rawRecord)
	if err != nil {
		writeErr(w, "PutIPNS", http.StatusBadRequest, fmt.Errorf("provided record is invalid: %w", err))
		return
	}

	err = ipns.ValidateWithName(record, name)
	if err != nil {
		writeErr(w, "PutIPNS", http.StatusBadRequest, fmt.Errorf("provided record is invalid: %w", err))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.routingTimeout)
	defer cancel()

	err = s.svc.PutIPNS(ctx, name, record)
	if err != nil {
		writeErr(w, "PutIPNS", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
		return
	}

	w.WriteHeader(http.StatusOK)
}

var (
	// Rule-of-thumb Cache-Control policy is to work well with caching proxies and load balancers.
	// If there are any results, cache on the client for longer, and hint any in-between caches to
	// serve cached result and upddate cache in background as long we have
	// result that is within Amino DHT expiration window
	maxAgeWithResults    = int((5 * time.Minute).Seconds())  // cache >0 results for longer
	maxAgeWithoutResults = int((15 * time.Second).Seconds()) // cache no results briefly
	maxStale             = int((48 * time.Hour).Seconds())   // allow stale results as long within Amino DHT  Expiration window
)

func setCacheControl(w http.ResponseWriter, maxAge int, stale int) {
	w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d, stale-while-revalidate=%d, stale-if-error=%d", maxAge, stale, stale))
}

func writeJSONResult(w http.ResponseWriter, method string, val interface{ Length() int }) {
	w.Header().Add("Content-Type", mediaTypeJSON)
	w.Header().Add("Vary", "Accept")

	if val.Length() > 0 {
		setCacheControl(w, maxAgeWithResults, maxStale)
	} else {
		setCacheControl(w, maxAgeWithoutResults, maxStale)
	}
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))

	// keep the marshaling separate from the writing, so we can distinguish bugs (which surface as 500)
	// from transient network issues (which surface as transport errors)
	b, err := drjson.MarshalJSONBytes(val)
	if err != nil {
		writeErr(w, method, http.StatusInternalServerError, fmt.Errorf("marshaling response: %w", err))
		return
	}

	if val.Length() > 0 {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}

	_, err = io.Copy(w, bytes.NewBuffer(b))
	if err != nil {
		logErr("Provide", "writing response body", err)
	}
}

func writeErr(w http.ResponseWriter, method string, statusCode int, cause error) {
	if errors.Is(cause, routing.ErrNotFound) {
		setCacheControl(w, maxAgeWithoutResults, maxStale)
	}

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

func writeResultsIterNDJSON[T types.Record](w http.ResponseWriter, resultIter iter.ResultIter[T]) {
	defer resultIter.Close()

	w.Header().Set("Content-Type", mediaTypeNDJSON)
	w.Header().Add("Vary", "Accept")
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))

	hasResults := false
	for resultIter.Next() {
		res := resultIter.Val()
		if res.Err != nil {
			logger.Errorw("ndjson iterator error", "Error", res.Err)
			return
		}

		// don't use an encoder because we can't easily differentiate writer errors from encoding errors
		b, err := drjson.MarshalJSONBytes(res.Val)
		if err != nil {
			logger.Errorw("ndjson marshal error", "Error", err)
			return
		}

		if !hasResults {
			hasResults = true
			// There's results, cache useful result for longer
			setCacheControl(w, maxAgeWithResults, maxStale)
		}

		_, err = w.Write(b)
		if err != nil {
			logger.Warn("ndjson write error", "Error", err)
			return
		}

		_, err = w.Write([]byte{'\n'})
		if err != nil {
			logger.Warn("ndjson write error", "Error", err)
			return
		}

		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}

	if !hasResults {
		// There weren't results, cache for shorter and send 404
		setCacheControl(w, maxAgeWithoutResults, maxStale)
		w.WriteHeader(http.StatusNotFound)
	}
}
