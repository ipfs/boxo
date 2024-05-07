package client

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

	"github.com/benbjohnson/clock"
	ipns "github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/contentrouter"
	"github.com/ipfs/boxo/routing/http/internal/drjson"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	jsontypes "github.com/ipfs/boxo/routing/http/types/json"
	"github.com/ipfs/boxo/routing/http/types/ndjson"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multibase"
)

var (
	_      contentrouter.Client = &Client{}
	logger                      = logging.Logger("routing/http/client")
)

const (
	mediaTypeJSON       = "application/json"
	mediaTypeNDJSON     = "application/x-ndjson"
	mediaTypeIPNSRecord = "application/vnd.ipfs.ipns-record"
)

type Client struct {
	baseURL    string
	httpClient httpClient
	clock      clock.Clock
	accepts    string

	identity  crypto.PrivKey
	peerID    peer.ID
	addrs     []types.Multiaddr
	protocols []string

	// Called immediately after signing a provide (peer) request. It is used
	// for testing, e.g., testing the server with a mangled signature.
	afterSignCallback func(req *types.AnnouncementRecord)
}

// defaultUserAgent is used as a fallback to inform HTTP server which library
// version sent a request
var defaultUserAgent = moduleVersion()

var _ contentrouter.Client = &Client{}

func newDefaultHTTPClient(userAgent string) *http.Client {
	return &http.Client{
		Transport: &ResponseBodyLimitedTransport{
			RoundTripper: http.DefaultTransport,
			LimitBytes:   1 << 20,
			UserAgent:    userAgent,
		},
	}
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Option func(*Client) error

func WithHTTPClient(h httpClient) Option {
	return func(c *Client) error {
		c.httpClient = h
		return nil
	}
}

// WithUserAgent sets a custom user agent to use with the HTTP Client. This modifies
// the underlying [http.Client]. Therefore, you should not use the same HTTP Client
// with multiple routing clients.
//
// This only works if using a [http.Client] with a [ResponseBodyLimitedTransport]
// set as its transport. Otherwise, an error will be returned.
func WithUserAgent(ua string) Option {
	return func(c *Client) error {
		if ua == "" {
			return errors.New("empty user agent")
		}
		httpClient, ok := c.httpClient.(*http.Client)
		if !ok {
			return errors.New("the http client of the Client must be a *http.Client")
		}
		transport, ok := httpClient.Transport.(*ResponseBodyLimitedTransport)
		if !ok {
			return errors.New("the transport of the http client of the Client must be a *ResponseBodyLimitedTransport")
		}
		transport.UserAgent = ua
		return nil
	}
}

// WithProviderInfo configures the [Client] with the given provider information.
// This is used by the methods [Client.Provide] and [Client.ProvidePeer] in order
// to create and sign announcement records.
//
// You can still use [Client.ProvideRecords] and [Client.ProvidePeerRecords]
// without this configuration. Then, you must provide already signed-records.
func WithProviderInfo(identity crypto.PrivKey, peerID peer.ID, addrs []multiaddr.Multiaddr, protocols []string) Option {
	return func(c *Client) error {
		c.identity = identity
		c.peerID = peerID
		c.protocols = protocols
		for _, a := range addrs {
			c.addrs = append(c.addrs, types.Multiaddr{Multiaddr: a})
		}
		return nil
	}
}

func WithStreamResultsRequired() Option {
	return func(c *Client) error {
		c.accepts = mediaTypeNDJSON
		return nil
	}
}

// New creates a content routing API client.
// The Provider and identity parameters are option. If they are nil, the [client.ProvideBitswap] method will not function.
func New(baseURL string, opts ...Option) (*Client, error) {
	client := &Client{
		baseURL:    baseURL,
		httpClient: newDefaultHTTPClient(defaultUserAgent),
		clock:      clock.New(),
		accepts:    strings.Join([]string{mediaTypeNDJSON, mediaTypeJSON}, ","),
	}

	for _, opt := range opts {
		err := opt(client)
		if err != nil {
			return nil, err
		}
	}

	if client.identity != nil && client.peerID.Size() != 0 && !client.peerID.MatchesPublicKey(client.identity.GetPublic()) {
		return nil, errors.New("identity does not match provider")
	}

	return client, nil
}

// measuringIter measures the length of the iter and then publishes metrics about the whole req once the iter is closed.
// Of course, if the caller forgets to close the iter, this won't publish anything.
type measuringIter[T any] struct {
	iter.Iter[T]
	ctx context.Context
	m   *measurement
}

func (c *measuringIter[T]) Next() bool {
	c.m.length++
	return c.Iter.Next()
}

func (c *measuringIter[T]) Val() T {
	return c.Iter.Val()
}

func (c *measuringIter[T]) Close() error {
	c.m.record(c.ctx)
	return c.Iter.Close()
}

// FindProviders searches for providers that are able to provide the given [cid.Cid].
// In a more generic way, it is also used as a mapping between CIDs and relevant metadata.
func (c *Client) FindProviders(ctx context.Context, key cid.Cid) (providers iter.ResultIter[types.Record], err error) {
	// TODO test measurements
	m := newMeasurement("FindProviders")

	url := c.baseURL + "/routing/v1/providers/" + key.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", c.accepts)

	m.host = req.Host

	start := c.clock.Now()
	resp, err := c.httpClient.Do(req)

	m.err = err
	m.latency = c.clock.Since(start)

	if err != nil {
		m.record(ctx)
		return nil, err
	}

	m.statusCode = resp.StatusCode
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		m.record(ctx)
		return iter.FromSlice[iter.Result[types.Record]](nil), nil
	}

	if resp.StatusCode != http.StatusOK {
		err := httpError(resp.StatusCode, resp.Body)
		resp.Body.Close()
		m.record(ctx)
		return nil, err
	}

	respContentType := resp.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(respContentType)
	if err != nil {
		resp.Body.Close()
		m.err = err
		m.record(ctx)
		return nil, fmt.Errorf("parsing Content-Type: %w", err)
	}

	m.mediaType = mediaType

	var skipBodyClose bool
	defer func() {
		if !skipBodyClose {
			resp.Body.Close()
		}
	}()

	var it iter.ResultIter[types.Record]
	switch mediaType {
	case mediaTypeJSON:
		parsedResp := &jsontypes.ProvidersResponse{}
		err = json.NewDecoder(resp.Body).Decode(parsedResp)
		var sliceIt iter.Iter[types.Record] = iter.FromSlice(parsedResp.Providers)
		it = iter.ToResultIter(sliceIt)
	case mediaTypeNDJSON:
		skipBodyClose = true
		it = ndjson.NewRecordsIter(resp.Body)
	default:
		logger.Errorw("unknown media type", "MediaType", mediaType, "ContentType", respContentType)
		return nil, errors.New("unknown content type")
	}

	return &measuringIter[iter.Result[types.Record]]{Iter: it, ctx: ctx, m: m}, nil
}

// Provide publishes [types.AnnouncementRecord]s based on the given [types.AnnouncementRequests].
// This records will be signed by your provided. Therefore, the [Client] must have been configured
// with [WithProviderInfo].
func (c *Client) Provide(ctx context.Context, announcements ...types.AnnouncementRequest) (iter.ResultIter[*types.AnnouncementResponseRecord], error) {
	if err := c.canProvide(); err != nil {
		return nil, err
	}

	now := c.clock.Now()
	records := make([]*types.AnnouncementRecord, len(announcements))

	for i, announcement := range announcements {
		record := &types.AnnouncementRecord{
			Schema: types.SchemaAnnouncement,
			Payload: types.AnnouncementPayload{
				CID:       announcement.CID,
				Scope:     announcement.Scope,
				Timestamp: now,
				TTL:       announcement.TTL,
				ID:        &c.peerID,
				Addrs:     c.addrs,
				Protocols: c.protocols,
			},
		}

		if len(announcement.Metadata) != 0 {
			var err error
			record.Payload.Metadata, err = multibase.Encode(multibase.Base64, announcement.Metadata)
			if err != nil {
				return nil, fmt.Errorf("multibase-encoding metadata: %w", err)
			}
		}

		err := record.Sign(c.peerID, c.identity)
		if err != nil {
			return nil, err
		}

		if c.afterSignCallback != nil {
			c.afterSignCallback(record)
		}

		records[i] = record
	}

	url := c.baseURL + "/routing/v1/providers"
	req := jsontypes.AnnounceProvidersRequest{
		Providers: records,
	}
	return c.provide(ctx, url, req)
}

// ProvideRecords publishes the given [types.AnnouncementRecord]. An error will
// be returned if the records aren't signed or valid.
func (c *Client) ProvideRecords(ctx context.Context, records ...*types.AnnouncementRecord) (iter.ResultIter[*types.AnnouncementResponseRecord], error) {
	providerRecords := make([]*types.AnnouncementRecord, len(records))
	for i, record := range records {
		if err := record.Verify(); err != nil {
			return nil, err
		}
		providerRecords[i] = records[i]
	}

	url := c.baseURL + "/routing/v1/providers"
	req := jsontypes.AnnounceProvidersRequest{
		Providers: providerRecords,
	}
	return c.provide(ctx, url, req)
}

func (c *Client) provide(ctx context.Context, url string, req interface{}) (iter.ResultIter[*types.AnnouncementResponseRecord], error) {
	b, err := drjson.MarshalJSONBytes(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("making HTTP req to provide a signed peer record: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, httpError(resp.StatusCode, resp.Body)
	}

	respContentType := resp.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(respContentType)
	if err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("parsing Content-Type: %w", err)
	}

	var skipBodyClose bool
	defer func() {
		if !skipBodyClose {
			resp.Body.Close()
		}
	}()

	var it iter.ResultIter[*types.AnnouncementResponseRecord]
	switch mediaType {
	case mediaTypeJSON:
		parsedResp := &jsontypes.AnnouncePeersResponse{}
		err = json.NewDecoder(resp.Body).Decode(parsedResp)
		if err != nil {
			return nil, err
		}
		var sliceIt iter.Iter[*types.AnnouncementResponseRecord] = iter.FromSlice(parsedResp.ProvideResults)
		it = iter.ToResultIter(sliceIt)
	case mediaTypeNDJSON:
		skipBodyClose = true
		it = ndjson.NewAnnouncementResponseRecordsIter(resp.Body)
	default:
		logger.Errorw("unknown media type", "MediaType", mediaType, "ContentType", respContentType)
		return nil, errors.New("unknown content type")
	}

	return it, nil
}

func (c *Client) canProvide() error {
	if c.identity == nil {
		return errors.New("cannot provide without identity")
	}
	if c.peerID.Size() == 0 {
		return errors.New("cannot provide without peer ID")
	}
	return nil
}

// FindPeers searches for information for the given [peer.ID].
func (c *Client) FindPeers(ctx context.Context, pid peer.ID) (peers iter.ResultIter[*types.PeerRecord], err error) {
	m := newMeasurement("FindPeers")

	url := c.baseURL + "/routing/v1/peers/" + peer.ToCid(pid).String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", c.accepts)

	m.host = req.Host

	start := c.clock.Now()
	resp, err := c.httpClient.Do(req)

	m.err = err
	m.latency = c.clock.Since(start)

	if err != nil {
		m.record(ctx)
		return nil, err
	}

	m.statusCode = resp.StatusCode
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		m.record(ctx)
		return iter.FromSlice[iter.Result[*types.PeerRecord]](nil), nil
	}

	if resp.StatusCode != http.StatusOK {
		err := httpError(resp.StatusCode, resp.Body)
		resp.Body.Close()
		m.record(ctx)
		return nil, err
	}

	respContentType := resp.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(respContentType)
	if err != nil {
		resp.Body.Close()
		m.err = err
		m.record(ctx)
		return nil, fmt.Errorf("parsing Content-Type: %w", err)
	}

	m.mediaType = mediaType

	var skipBodyClose bool
	defer func() {
		if !skipBodyClose {
			resp.Body.Close()
		}
	}()

	var it iter.ResultIter[*types.PeerRecord]
	switch mediaType {
	case mediaTypeJSON:
		parsedResp := &jsontypes.PeersResponse{}
		err = json.NewDecoder(resp.Body).Decode(parsedResp)
		if err != nil {
			return nil, err
		}
		var sliceIt iter.Iter[*types.PeerRecord] = iter.FromSlice(parsedResp.Peers)
		it = iter.ToResultIter(sliceIt)
	case mediaTypeNDJSON:
		skipBodyClose = true
		it = ndjson.NewPeerRecordsIter(resp.Body)
	default:
		logger.Errorw("unknown media type", "MediaType", mediaType, "ContentType", respContentType)
		return nil, errors.New("unknown content type")
	}

	return &measuringIter[iter.Result[*types.PeerRecord]]{Iter: it, ctx: ctx, m: m}, nil
}

// ProvidePeer publishes an [types.AnnouncementRecord] with the provider
// information from your peer, configured with [WithProviderInfo].
func (c *Client) ProvidePeer(ctx context.Context, ttl time.Duration, metadata []byte) (iter.ResultIter[*types.AnnouncementResponseRecord], error) {
	if err := c.canProvide(); err != nil {
		return nil, err
	}

	record := &types.AnnouncementRecord{
		Schema: types.SchemaAnnouncement,
		Payload: types.AnnouncementPayload{
			Timestamp: time.Now(),
			TTL:       ttl,
			ID:        &c.peerID,
			Addrs:     c.addrs,
			Protocols: c.protocols,
		},
	}

	if len(metadata) != 0 {
		var err error
		record.Payload.Metadata, err = multibase.Encode(multibase.Base64, metadata)
		if err != nil {
			return nil, fmt.Errorf("multibase-encoding metadata: %w", err)
		}
	}

	err := record.Sign(c.peerID, c.identity)
	if err != nil {
		return nil, err
	}

	if c.afterSignCallback != nil {
		c.afterSignCallback(record)
	}

	url := c.baseURL + "/routing/v1/peers"
	req := jsontypes.AnnouncePeersRequest{
		Peers: []*types.AnnouncementRecord{record},
	}

	return c.provide(ctx, url, req)
}

// ProvidePeerRecords publishes the given [types.AnnouncementRecord]. An error will
// be returned if the records aren't signed or valid.
func (c *Client) ProvidePeerRecords(ctx context.Context, records ...*types.AnnouncementRecord) (iter.ResultIter[*types.AnnouncementResponseRecord], error) {
	providerRecords := make([]*types.AnnouncementRecord, len(records))
	for i, record := range records {
		if err := record.Verify(); err != nil {
			return nil, err
		}
		providerRecords[i] = records[i]
	}

	url := c.baseURL + "/routing/v1/peers"
	req := jsontypes.AnnouncePeersRequest{
		Peers: providerRecords,
	}
	return c.provide(ctx, url, req)
}

// GetIPNS tries to retrieve the [ipns.Record] for the given [ipns.Name]. The record is
// validated against the given name. If validation fails, an error is returned, but no
// record.
func (c *Client) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	url := c.baseURL + "/routing/v1/ipns/" + name.String()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Accept", mediaTypeIPNSRecord)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("making HTTP req to get IPNS record: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httpError(resp.StatusCode, resp.Body)
	}

	// Limit the reader to the maximum record size.
	rawRecord, err := io.ReadAll(io.LimitReader(resp.Body, int64(ipns.MaxRecordSize)))
	if err != nil {
		return nil, fmt.Errorf("making HTTP req to get IPNS record: %w", err)
	}

	record, err := ipns.UnmarshalRecord(rawRecord)
	if err != nil {
		return nil, fmt.Errorf("IPNS record from remote endpoint is not valid: %w", err)
	}

	err = ipns.ValidateWithName(record, name)
	if err != nil {
		return nil, fmt.Errorf("IPNS record from remote endpoint is not valid: %w", err)
	}

	return record, nil
}

// PutIPNS attempts at putting the given [ipns.Record] for the given [ipns.Name].
func (c *Client) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	url := c.baseURL + "/routing/v1/ipns/" + name.String()

	rawRecord, err := ipns.MarshalRecord(record)
	if err != nil {
		return err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(rawRecord))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", mediaTypeIPNSRecord)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("making HTTP req to get IPNS record: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return httpError(resp.StatusCode, resp.Body)
	}

	return nil
}
