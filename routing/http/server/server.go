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
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/gorilla/mux"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/internal/drjson"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	jsontypes "github.com/ipfs/boxo/routing/http/types/json"
	"github.com/ipfs/boxo/routing/http/types/ndjson"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multibase"

	logging "github.com/ipfs/go-log/v2"
)

const (
	mediaTypeJSON       = "application/json"
	mediaTypeNDJSON     = "application/x-ndjson"
	mediaTypeWildcard   = "*/*"
	mediaTypeIPNSRecord = "application/vnd.ipfs.ipns-record"

	DefaultRecordsLimit          = 20
	DefaultStreamingRecordsLimit = 0
)

var logger = logging.Logger("routing/http/server")

const (
	providePath       = "/routing/v1/providers"
	findProvidersPath = "/routing/v1/providers/{cid}"
	providePeersPath  = "/routing/v1/peers"
	findPeersPath     = "/routing/v1/peers/{peer-id}"
	getIPNSPath       = "/routing/v1/ipns/{cid}"
)

type FindProvidersAsyncResponse struct {
	ProviderResponse types.Record
	Error            error
}

type ContentRouter interface {
	// FindProviders searches for peers who are able to provide the given [cid.Cid].
	// Limit indicates the maximum amount of results to return; 0 means unbounded.
	FindProviders(ctx context.Context, cid cid.Cid, limit int) (iter.ResultIter[types.Record], error)

	// Provide stores the provided [types.AnnouncementRecord] record for CIDs. Can return
	// a different TTL than the provided.
	Provide(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error)

	// FindPeers searches for peers who have the provided [peer.ID].
	// Limit indicates the maximum amount of results to return; 0 means unbounded.
	FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error)

	// ProvidePeer stores the provided [types.AnnouncementRecord] record for peers. Can
	// return a different TTL than the provided.
	ProvidePeer(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error)

	// GetIPNS searches for an [ipns.Record] for the given [ipns.Name].
	GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error)

	// PutIPNS stores the provided [ipns.Record] for the given [ipns.Name].
	// It is guaranteed that the record matches the provided name.
	PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error
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

func Handler(svc ContentRouter, opts ...Option) http.Handler {
	server := &server{
		svc:                   svc,
		recordsLimit:          DefaultRecordsLimit,
		streamingRecordsLimit: DefaultStreamingRecordsLimit,
	}

	for _, opt := range opts {
		opt(server)
	}

	r := mux.NewRouter()
	r.HandleFunc(findProvidersPath, server.findProviders).Methods(http.MethodGet)
	r.HandleFunc(providePath, server.provide).Methods(http.MethodPost)
	r.HandleFunc(findPeersPath, server.findPeers).Methods(http.MethodGet)
	r.HandleFunc(providePeersPath, server.providePeers).Methods(http.MethodPost)
	r.HandleFunc(getIPNSPath, server.GetIPNS).Methods(http.MethodGet)
	r.HandleFunc(getIPNSPath, server.PutIPNS).Methods(http.MethodPut)
	return r
}

type server struct {
	svc                   ContentRouter
	disableNDJSON         bool
	recordsLimit          int
	streamingRecordsLimit int
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

	mediaType, err := s.detectResponseType(httpReq)
	if err != nil {
		writeErr(w, "FindProviders", http.StatusBadRequest, err)
		return
	}

	var (
		handlerFunc  func(w http.ResponseWriter, provIter iter.ResultIter[types.Record])
		recordsLimit int
	)

	if mediaType == mediaTypeNDJSON {
		handlerFunc = s.findProvidersNDJSON
		recordsLimit = s.streamingRecordsLimit
	} else {
		handlerFunc = s.findProvidersJSON
		recordsLimit = s.recordsLimit
	}

	provIter, err := s.svc.FindProviders(httpReq.Context(), cid, recordsLimit)
	if err != nil {
		if errors.Is(err, routing.ErrNotFound) {
			// handlerFunc takes care of setting the 404 and necessary headers
			provIter = iter.FromSlice([]iter.Result[types.Record]{})
		} else {
			writeErr(w, "FindProviders", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
			return
		}
	}

	handlerFunc(w, provIter)
}

func (s *server) findProvidersJSON(w http.ResponseWriter, provIter iter.ResultIter[types.Record]) {
	defer provIter.Close()

	providers, err := iter.ReadAllResults(provIter)
	if err != nil {
		writeErr(w, "FindProviders", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
		return
	}

	writeJSONResult(w, "FindProviders", jsontypes.ProvidersResponse{
		Providers: providers,
	})
}

func (s *server) findProvidersNDJSON(w http.ResponseWriter, provIter iter.ResultIter[types.Record]) {
	writeResultsIterNDJSON(w, provIter)
}

func (s *server) findPeers(w http.ResponseWriter, r *http.Request) {
	pidStr := mux.Vars(r)["peer-id"]

	// While specification states that peer-id is expected to be in CIDv1 format, reality
	// is the clients will often learn legacy PeerID string from other sources,
	// and try to use it.
	// See https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md#string-representation
	// We are liberal in inputs here, and uplift legacy PeerID to CID if necessary.
	// Rationale: it is better to fix this common mistake than to error and break peer routing.
	cid, err := cid.Decode(pidStr)
	if err != nil {
		// check if input is peer ID in legacy format
		if pid, err2 := peer.Decode(pidStr); err2 == nil {
			cid = peer.ToCid(pid)
		} else {
			writeErr(w, "FindPeers", http.StatusBadRequest, fmt.Errorf("unable to parse peer ID as libp2p-key CID: %w", err))
			return
		}
	}

	pid, err := peer.FromCid(cid)
	if err != nil {
		writeErr(w, "FindPeers", http.StatusBadRequest, fmt.Errorf("unable to parse peer ID: %w", err))
		return
	}

	mediaType, err := s.detectResponseType(r)
	if err != nil {
		writeErr(w, "FindPeers", http.StatusBadRequest, err)
		return
	}

	var (
		handlerFunc  func(w http.ResponseWriter, provIter iter.ResultIter[*types.PeerRecord])
		recordsLimit int
	)

	if mediaType == mediaTypeNDJSON {
		handlerFunc = s.findPeersNDJSON
		recordsLimit = s.streamingRecordsLimit
	} else {
		handlerFunc = s.findPeersJSON
		recordsLimit = s.recordsLimit
	}

	provIter, err := s.svc.FindPeers(r.Context(), pid, recordsLimit)
	if err != nil {
		if errors.Is(err, routing.ErrNotFound) {
			// handlerFunc takes care of setting the 404 and necessary headers
			provIter = iter.FromSlice([]iter.Result[*types.PeerRecord]{})
		} else {
			writeErr(w, "FindPeers", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
			return
		}
	}

	handlerFunc(w, provIter)
}

func (s *server) providePeers(w http.ResponseWriter, r *http.Request) {
	var requestIter iter.ResultIter[*types.AnnouncementRecord]
	if r.Header.Get("Content-Type") == mediaTypeNDJSON {
		requestIter = ndjson.NewAnnouncementRecordsIter(r.Body)
	} else {
		req := jsontypes.AnnouncePeersRequest{}
		err := json.NewDecoder(r.Body).Decode(&req)
		_ = r.Body.Close()
		if err != nil {
			writeErr(w, "Provide", http.StatusBadRequest, fmt.Errorf("invalid request: %w", err))
			return
		}
		requestIter = iter.ToResultIter(iter.FromSlice(req.Peers))
	}

	responseIter := iter.Map(requestIter, func(t iter.Result[*types.AnnouncementRecord]) *types.AnnouncementResponseRecord {
		resRecord := &types.AnnouncementResponseRecord{
			Schema: types.SchemaAnnouncementResponse,
		}

		if t.Err != nil {
			resRecord.Error = t.Err.Error()
			return resRecord
		}

		err := t.Val.Verify()
		if err != nil {
			resRecord.Error = fmt.Sprintf("Provide: signature verification failed: %s", err)
			return resRecord
		}

		ttl, err := s.svc.ProvidePeer(r.Context(), t.Val)
		if err != nil {
			resRecord.Error = err.Error()
			return resRecord
		}

		resRecord.TTL = ttl
		return resRecord
	})

	mediaType, err := s.detectResponseType(r)
	if err != nil {
		writeErr(w, "FindPeers", http.StatusBadRequest, err)
		return
	}

	if mediaType == mediaTypeNDJSON {
		writeResultsIterNDJSON(w, iter.ToResultIter(responseIter))
	} else {
		writeJSONResult(w, "ProvidePeers", jsontypes.AnnouncePeersResponse{
			ProvideResults: iter.ReadAll(responseIter),
		})
	}
}

func (s *server) provide(w http.ResponseWriter, r *http.Request) {
	var requestIter iter.ResultIter[*types.AnnouncementRecord]
	if r.Header.Get("Content-Type") == mediaTypeNDJSON {
		requestIter = ndjson.NewAnnouncementRecordsIter(r.Body)
	} else {
		req := jsontypes.AnnounceProvidersRequest{}
		err := json.NewDecoder(r.Body).Decode(&req)
		_ = r.Body.Close()
		if err != nil {
			writeErr(w, "Provide", http.StatusBadRequest, fmt.Errorf("invalid request: %w", err))
			return
		}
		requestIter = iter.ToResultIter(iter.FromSlice(req.Providers))
	}

	responseIter := iter.Map(requestIter, func(t iter.Result[*types.AnnouncementRecord]) *types.AnnouncementResponseRecord {
		resRecord := &types.AnnouncementResponseRecord{
			Schema: types.SchemaAnnouncementResponse,
		}

		if t.Err != nil {
			resRecord.Error = t.Err.Error()
			return resRecord
		}

		err := t.Val.Verify()
		if err != nil {
			resRecord.Error = fmt.Sprintf("Provide: signature verification failed: %s", err)
			return resRecord
		}

		ttl, err := s.svc.Provide(r.Context(), t.Val)
		if err != nil {
			resRecord.Error = err.Error()
			return resRecord
		}

		resRecord.TTL = ttl
		return resRecord
	})

	mediaType, err := s.detectResponseType(r)
	if err != nil {
		writeErr(w, "FindPeers", http.StatusBadRequest, err)
		return
	}

	if mediaType == mediaTypeNDJSON {
		writeResultsIterNDJSON(w, iter.ToResultIter(responseIter))
	} else {
		writeJSONResult(w, "Provide", jsontypes.AnnounceProvidersResponse{
			ProvideResults: iter.ReadAll(responseIter),
		})
	}
}

func (s *server) findPeersJSON(w http.ResponseWriter, peersIter iter.ResultIter[*types.PeerRecord]) {
	defer peersIter.Close()

	peers, err := iter.ReadAllResults(peersIter)
	if err != nil {
		writeErr(w, "FindPeers", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
		return
	}

	writeJSONResult(w, "FindPeers", jsontypes.PeersResponse{
		Peers: peers,
	})
}

func (s *server) findPeersNDJSON(w http.ResponseWriter, peersIter iter.ResultIter[*types.PeerRecord]) {
	writeResultsIterNDJSON(w, peersIter)
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

	record, err := s.svc.GetIPNS(r.Context(), name)
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

	err = s.svc.PutIPNS(r.Context(), name, record)
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

func writeResultsIterNDJSON[T any](w http.ResponseWriter, resultIter iter.ResultIter[T]) {
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
