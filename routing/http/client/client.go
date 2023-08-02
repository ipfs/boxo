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

	"github.com/benbjohnson/clock"
	ipns "github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/contentrouter"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	jsontypes "github.com/ipfs/boxo/routing/http/types/json"
	"github.com/ipfs/boxo/routing/http/types/ndjson"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	_                 contentrouter.Client = &client{}
	logger                                 = logging.Logger("routing/http/client")
	defaultHTTPClient                      = &http.Client{
		Transport: &ResponseBodyLimitedTransport{
			RoundTripper: http.DefaultTransport,
			LimitBytes:   1 << 20,
			UserAgent:    defaultUserAgent,
		},
	}
)

const (
	mediaTypeJSON       = "application/json"
	mediaTypeNDJSON     = "application/x-ndjson"
	mediaTypeIPNSRecord = "application/vnd.ipfs.ipns-record"
)

type client struct {
	baseURL    string
	httpClient httpClient
	clock      clock.Clock
	accepts    string
}

// defaultUserAgent is used as a fallback to inform HTTP server which library
// version sent a request
var defaultUserAgent = moduleVersion()

var _ contentrouter.Client = &client{}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Option func(*client)

func WithHTTPClient(h httpClient) Option {
	return func(c *client) {
		c.httpClient = h
	}
}

func WithUserAgent(ua string) Option {
	return func(c *client) {
		if ua == "" {
			return
		}
		httpClient, ok := c.httpClient.(*http.Client)
		if !ok {
			return
		}
		transport, ok := httpClient.Transport.(*ResponseBodyLimitedTransport)
		if !ok {
			return
		}
		transport.UserAgent = ua
	}
}

func WithStreamResultsRequired() Option {
	return func(c *client) {
		c.accepts = mediaTypeNDJSON
	}
}

// New creates a content routing API client.
func New(baseURL string, opts ...Option) (*client, error) {
	client := &client{
		baseURL:    baseURL,
		httpClient: defaultHTTPClient,
		clock:      clock.New(),
		accepts:    strings.Join([]string{mediaTypeNDJSON, mediaTypeJSON}, ","),
	}

	for _, opt := range opts {
		opt(client)
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

func (c *client) FindProviders(ctx context.Context, key cid.Cid) (provs iter.ResultIter[types.Record], err error) {
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
		it = ndjson.NewProvidersResponseIter(resp.Body)
	default:
		logger.Errorw("unknown media type", "MediaType", mediaType, "ContentType", respContentType)
		return nil, errors.New("unknown content type")
	}

	return &measuringIter[iter.Result[types.Record]]{Iter: it, ctx: ctx, m: m}, nil
}

func (c *client) FindIPNSRecord(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
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

func (c *client) ProvideIPNSRecord(ctx context.Context, name ipns.Name, record *ipns.Record) error {
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
