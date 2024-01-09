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
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/gorilla/mux"
	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/internal/drjson"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	jsontypes "github.com/ipfs/boxo/routing/http/types/json"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

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
	providePath       = "/routing/v1/providers" // TODO: with trailing slash?
	findProvidersPath = "/routing/v1/providers/{cid}"
	providePeersPath  = "/routing/v1/peers" // TODO: with trailing slash?
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

	// Provide stores the provided [ProvideRequest] record for CIDs. Can return
	// a different TTL than the provided.
	Provide(ctx context.Context, req *ProvideRequest) (time.Duration, error)

	// FindPeers searches for peers who have the provided [peer.ID].
	// Limit indicates the maximum amount of results to return; 0 means unbounded.
	FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error)

	// ProvidePeer stores the provided [ProvidePeerRequest] record for peers. Can
	// return a different TTL than the provided.
	ProvidePeer(ctx context.Context, req *ProvidePeerRequest) (time.Duration, error)

	// GetIPNS searches for an [ipns.Record] for the given [ipns.Name].
	GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error)

	// PutIPNS stores the provided [ipns.Record] for the given [ipns.Name].
	// It is guaranteed that the record matches the provided name.
	PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error
}

// ProvideRequest is a content provide request.
type ProvideRequest struct {
	CID       cid.Cid
	Scope     types.AnnouncementScope
	Timestamp time.Time
	TTL       time.Duration
	ID        peer.ID
	Addrs     []multiaddr.Multiaddr
	Protocols []string
	Metadata  string
}

// ProvidePeerRequest is a peer provide request.
type ProvidePeerRequest struct {
	Timestamp time.Time
	TTL       time.Duration
	ID        peer.ID
	Addrs     []multiaddr.Multiaddr
	Protocols []string
	Metadata  string
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
	r.HandleFunc(providePath, server.provide).Methods(http.MethodPut)
	r.HandleFunc(findPeersPath, server.findPeers).Methods(http.MethodGet)
	r.HandleFunc(providePeersPath, server.providePeers).Methods(http.MethodPut)
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
		writeErr(w, "FindProviders", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
		return
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

	// pidStr must be in CIDv1 format. Therefore, use [cid.Decode]. We can't use
	// [peer.Decode] because that would allow other formats to pass through.
	cid, err := cid.Decode(pidStr)
	if err != nil {
		if pid, err := peer.Decode(pidStr); err == nil {
			writeErr(w, "FindPeers", http.StatusBadRequest, fmt.Errorf("the value is a peer ID, try using its CID representation: %s", peer.ToCid(pid).String()))
		} else {
			writeErr(w, "FindPeers", http.StatusBadRequest, fmt.Errorf("unable to parse peer ID: %w", err))
		}
		return
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
		writeErr(w, "FindPeers", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
		return
	}

	handlerFunc(w, provIter)
}

func (s *server) providePeers(w http.ResponseWriter, r *http.Request) {
	req := jsontypes.AnnouncePeersRequest{}
	err := json.NewDecoder(r.Body).Decode(&req)
	_ = r.Body.Close()
	if err != nil {
		writeErr(w, "ProvidePeers", http.StatusBadRequest, fmt.Errorf("invalid request: %w", err))
		return
	}

	responseIter := iter.Map[types.Record, *types.AnnouncementRecord](iter.FromSlice(req.Providers), func(t types.Record) *types.AnnouncementRecord {
		resRecord := &types.AnnouncementRecord{
			Schema: types.SchemaAnnouncement,
		}

		reqRecord, err := s.provideCheckAnnouncement("Provide", t)
		if err != nil {
			resRecord.Error = err.Error()
			return resRecord
		}

		req := &ProvidePeerRequest{
			Timestamp: reqRecord.Payload.Timestamp,
			TTL:       reqRecord.Payload.TTL,
			ID:        *reqRecord.Payload.ID,
			Addrs:     make([]multiaddr.Multiaddr, len(reqRecord.Payload.Addrs)),
			Protocols: reqRecord.Payload.Protocols,
			Metadata:  reqRecord.Payload.Metadata,
		}

		for i, addr := range reqRecord.Payload.Addrs {
			req.Addrs[i] = addr.Multiaddr
		}

		ttl, err := s.svc.ProvidePeer(r.Context(), req)
		if err != nil {
			resRecord.Error = err.Error()
			return resRecord
		}

		resRecord.Payload.TTL = ttl
		resRecord.Payload.ID = &req.ID
		return resRecord
	})

	mediaType, err := s.detectResponseType(r)
	if err != nil {
		writeErr(w, "FindPeers", http.StatusBadRequest, err)
		return
	}

	if mediaType == mediaTypeNDJSON {
		writeResultsIterNDJSON[*types.AnnouncementRecord](w, iter.ToResultIter[*types.AnnouncementRecord](responseIter))
	} else {
		writeJSONResult(w, "ProvidePeers", jsontypes.AnnouncePeersResponse{
			ProvideResults: iter.ReadAll[*types.AnnouncementRecord](responseIter),
		})
	}
}

func (s *server) provide(w http.ResponseWriter, r *http.Request) {
	req := jsontypes.AnnounceProvidersRequest{}
	err := json.NewDecoder(r.Body).Decode(&req)
	_ = r.Body.Close()
	if err != nil {
		writeErr(w, "Provide", http.StatusBadRequest, fmt.Errorf("invalid request: %w", err))
		return
	}

	responseIter := iter.Map[types.Record, *types.AnnouncementRecord](iter.FromSlice(req.Providers), func(t types.Record) *types.AnnouncementRecord {
		resRecord := &types.AnnouncementRecord{
			Schema: types.SchemaAnnouncement,
		}

		reqRecord, err := s.provideCheckAnnouncement("Provide", t)
		if err != nil {
			resRecord.Error = err.Error()
			return resRecord
		}

		req := &ProvideRequest{
			CID:       reqRecord.Payload.CID,
			Scope:     reqRecord.Payload.Scope,
			Timestamp: reqRecord.Payload.Timestamp,
			TTL:       reqRecord.Payload.TTL,
			ID:        *reqRecord.Payload.ID,
			Addrs:     make([]multiaddr.Multiaddr, len(reqRecord.Payload.Addrs)),
			Protocols: reqRecord.Payload.Protocols,
			Metadata:  reqRecord.Payload.Metadata,
		}

		for i, addr := range reqRecord.Payload.Addrs {
			req.Addrs[i] = addr.Multiaddr
		}

		ttl, err := s.svc.Provide(r.Context(), req)
		if err != nil {
			resRecord.Error = err.Error()
			return resRecord
		}

		resRecord.Payload.TTL = ttl
		resRecord.Payload.CID = req.CID
		resRecord.Payload.ID = &req.ID
		return resRecord
	})

	mediaType, err := s.detectResponseType(r)
	if err != nil {
		writeErr(w, "FindPeers", http.StatusBadRequest, err)
		return
	}

	if mediaType == mediaTypeNDJSON {
		writeResultsIterNDJSON[*types.AnnouncementRecord](w, iter.ToResultIter[*types.AnnouncementRecord](responseIter))
	} else {
		writeJSONResult(w, "Provide", jsontypes.AnnounceProvidersResponse{
			ProvideResults: iter.ReadAll[*types.AnnouncementRecord](responseIter),
		})
	}
}

func (s *server) provideCheckAnnouncement(method string, r types.Record) (*types.AnnouncementRecord, error) {
	if r.GetSchema() != types.SchemaAnnouncement {
		return nil, fmt.Errorf("%s: invalid schema %s", method, r.GetSchema())
	}

	rec, ok := r.(*types.AnnouncementRecord)
	if !ok {
		return nil, fmt.Errorf("%s: invalid type", method)
	}

	err := rec.Verify()
	if err != nil {
		return nil, fmt.Errorf("%s: signature verification failed: %w", method, err)
	}

	return rec, nil
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
	if !strings.Contains(r.Header.Get("Accept"), mediaTypeIPNSRecord) {
		writeErr(w, "GetIPNS", http.StatusNotAcceptable, errors.New("content type in 'Accept' header is missing or not supported"))
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
		writeErr(w, "GetIPNS", http.StatusInternalServerError, fmt.Errorf("delegate error: %w", err))
		return
	}

	rawRecord, err := ipns.MarshalRecord(record)
	if err != nil {
		writeErr(w, "GetIPNS", http.StatusInternalServerError, err)
		return
	}

	if ttl, err := record.TTL(); err == nil {
		w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%d", int(ttl.Seconds())))
	} else {
		w.Header().Set("Cache-Control", "max-age=60")
	}

	recordEtag := strconv.FormatUint(xxhash.Sum64(rawRecord), 32)
	w.Header().Set("Etag", recordEtag)
	w.Header().Set("Content-Type", mediaTypeIPNSRecord)
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

func writeJSONResult(w http.ResponseWriter, method string, val any) {
	w.Header().Add("Content-Type", mediaTypeJSON)

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

func writeResultsIterNDJSON[T any](w http.ResponseWriter, resultIter iter.ResultIter[T]) {
	defer resultIter.Close()

	w.Header().Set("Content-Type", mediaTypeNDJSON)
	w.WriteHeader(http.StatusOK)

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
}
